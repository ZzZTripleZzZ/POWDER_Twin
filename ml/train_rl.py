"""
W8 — PPO Fine-tuning on Twin (Contribution 3, Step 2).

Warm-starts from BC checkpoint, then fine-tunes with PPO using the twin
as a safe training environment. Periodically evaluates the current policy
through the PolicyArbiter safety filter before considering real deployment.

Usage:
    python ml/train_rl.py \
        --twin-host powder-twin \
        --real-host powder-gnb \
        --bc-checkpoint checkpoints/bc_policy.pt \
        --out checkpoints/ppo_policy.pt \
        [--n-ue 1] [--total-steps 50000] [--n-replicas 4]
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent.parent))
from ml.rl_env import TwinRanEnv
from ml.bc_pretrain import PolicyNet
from orchestrator.policy_arbiter import PolicyArbiter, Policy, SafetyCriterion


# ── Minimal PPO implementation ────────────────────────────────────────────────

class ValueNet(torch.nn.Module):
    def __init__(self, obs_dim: int, hidden: int = 128):
        super().__init__()
        self.net = torch.nn.Sequential(
            torch.nn.Linear(obs_dim, hidden), torch.nn.ReLU(),
            torch.nn.Linear(hidden, hidden),  torch.nn.ReLU(),
            torch.nn.Linear(hidden, 1),
        )

    def forward(self, x):
        return self.net(x).squeeze(-1)


class PPOTrainer:
    def __init__(
        self,
        env: TwinRanEnv,
        policy: PolicyNet,
        lr: float = 3e-4,
        gamma: float = 0.99,
        gae_lambda: float = 0.95,
        clip_eps: float = 0.2,
        n_epochs: int = 4,
        rollout_steps: int = 512,
        device: str = "cpu",
    ):
        self.env     = env
        self.policy  = policy.to(device)
        self.value   = ValueNet(policy.net[0].in_features).to(device)
        self.opt_p   = torch.optim.Adam(policy.parameters(), lr=lr)
        self.opt_v   = torch.optim.Adam(self.value.parameters(), lr=lr)
        self.gamma   = gamma
        self.gae_lam = gae_lambda
        self.clip    = clip_eps
        self.n_ep    = n_epochs
        self.n_roll  = rollout_steps
        self.device  = device
        self._total_steps = 0

    def _collect_rollout(self):
        obs_buf, act_buf, logp_buf, rew_buf, val_buf, done_buf = [], [], [], [], [], []
        obs, _ = self.env.reset()
        for _ in range(self.n_roll):
            obs_t = torch.tensor(obs, dtype=torch.float32, device=self.device).unsqueeze(0)
            with torch.no_grad():
                probs = self.policy(obs_t)[0]
                dist  = torch.distributions.Dirichlet(probs * 10 + 1e-6)
                act   = dist.sample()
                logp  = dist.log_prob(act)
                val   = self.value(obs_t)[0]

            next_obs, rew, term, trunc, _ = self.env.step(act.cpu().numpy())
            obs_buf.append(obs);  act_buf.append(act.cpu().numpy())
            logp_buf.append(logp.item()); rew_buf.append(rew)
            val_buf.append(val.item());  done_buf.append(term or trunc)

            obs = next_obs
            self._total_steps += 1
            if term or trunc:
                obs, _ = self.env.reset()

        # GAE advantage
        advantages, returns = [], []
        gae = 0.0
        obs_t = torch.tensor(obs, dtype=torch.float32, device=self.device).unsqueeze(0)
        with torch.no_grad():
            next_val = self.value(obs_t)[0].item()
        for t in reversed(range(self.n_roll)):
            nv = next_val if t == self.n_roll - 1 else val_buf[t + 1]
            mask = 0.0 if done_buf[t] else 1.0
            delta = rew_buf[t] + self.gamma * nv * mask - val_buf[t]
            gae   = delta + self.gamma * self.gae_lam * mask * gae
            advantages.insert(0, gae)
            returns.insert(0, gae + val_buf[t])

        to_t = lambda lst: torch.tensor(np.array(lst), dtype=torch.float32, device=self.device)
        return (to_t(obs_buf), to_t(act_buf), to_t(logp_buf),
                to_t(advantages), to_t(returns))

    def update(self):
        obs, acts, old_logp, adv, rets = self._collect_rollout()
        adv = (adv - adv.mean()) / (adv.std() + 1e-8)
        idx = torch.randperm(len(obs))

        pg_losses, v_losses = [], []
        for _ in range(self.n_ep):
            for start in range(0, len(obs), 64):
                b = idx[start:start+64]
                probs = self.policy(obs[b])
                dist  = torch.distributions.Dirichlet(probs * 10 + 1e-6)
                logp  = dist.log_prob(acts[b])
                ratio = torch.exp(logp - old_logp[b])
                pg    = -torch.min(ratio * adv[b],
                                   torch.clamp(ratio, 1-self.clip, 1+self.clip) * adv[b]).mean()
                vl    = (self.value(obs[b]) - rets[b]).pow(2).mean()
                self.opt_p.zero_grad(); pg.backward(); self.opt_p.step()
                self.opt_v.zero_grad(); vl.backward(); self.opt_v.step()
                pg_losses.append(pg.item()); v_losses.append(vl.item())

        return np.mean(pg_losses), np.mean(v_losses)


# ── Main training loop ────────────────────────────────────────────────────────

def train(
    twin_host: str,
    real_host: str,
    bc_ckpt: Path,
    out_path: Path,
    n_ue: int = 1,
    total_steps: int = 50_000,
    n_replicas: int = 4,
    deploy_every: int = 5000,
    device_str: str = "cpu",
):
    # Load BC warm-start
    ckpt = torch.load(bc_ckpt, map_location="cpu")
    obs_dim = ckpt["obs_dim"]
    policy  = PolicyNet(obs_dim, n_ue)
    policy.load_state_dict(ckpt["model"])
    print(f"Loaded BC checkpoint: obs_dim={obs_dim}, n_ue={n_ue}")

    env     = TwinRanEnv(twin_host=twin_host, n_ue=n_ue)
    trainer = PPOTrainer(env, policy, device=device_str)

    arbiter = PolicyArbiter(
        twin_host=twin_host,
        real_host=real_host,
        n_replicas=n_replicas,
        criterion=SafetyCriterion(bler_threshold=0.10, tpt_threshold=5.0),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    best_reward = -float("inf")
    last_deploy_step = 0

    print(f"Starting PPO fine-tuning for {total_steps} steps ...")
    while trainer._total_steps < total_steps:
        pg_loss, v_loss = trainer.update()
        steps = trainer._total_steps
        if steps % 2000 == 0:
            print(f"  Step {steps:6d}  pg={pg_loss:.4f}  v={v_loss:.4f}")

        # Periodic safety-filtered deployment attempt
        if steps - last_deploy_step >= deploy_every:
            last_deploy_step = steps
            obs_sample, _ = env.reset()
            obs_t  = torch.tensor(obs_sample, dtype=torch.float32).unsqueeze(0)
            with torch.no_grad():
                weights = policy(obs_t)[0].numpy()
            # Assume first n_ue RNTIs discovered by env
            rntis  = env._rntis[:n_ue] or [0x4401]
            p = Policy(weights={r: float(w) for r, w in zip(rntis, weights)})
            result = arbiter.evaluate(p)
            print(f"  [Arbiter] {result.summary()}")
            if result.safe:
                arbiter.deploy(p)
                torch.save({"model": policy.state_dict(), "obs_dim": obs_dim, "n_ue": n_ue,
                            "step": steps}, out_path)
                print(f"  Checkpoint saved → {out_path}")

    env.close()
    print("Training complete.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--twin-host",      required=True)
    parser.add_argument("--real-host",      required=True)
    parser.add_argument("--bc-checkpoint",  default="checkpoints/bc_policy.pt")
    parser.add_argument("--out",            default="checkpoints/ppo_policy.pt")
    parser.add_argument("--n-ue",           type=int, default=1)
    parser.add_argument("--total-steps",    type=int, default=50_000)
    parser.add_argument("--n-replicas",     type=int, default=4)
    parser.add_argument("--deploy-every",   type=int, default=5000)
    parser.add_argument("--device",         default="cpu")
    args = parser.parse_args()

    train(
        twin_host=args.twin_host,
        real_host=args.real_host,
        bc_ckpt=Path(args.bc_checkpoint),
        out_path=Path(args.out),
        n_ue=args.n_ue,
        total_steps=args.total_steps,
        n_replicas=args.n_replicas,
        deploy_every=args.deploy_every,
        device_str=args.device,
    )


if __name__ == "__main__":
    main()
