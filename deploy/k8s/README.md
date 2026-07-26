# Kubernetes deployment

Minimal GitOps-friendly manifests for running remux on Kubernetes as a
single-replica stateful singleton. The server's file/env-driven `Config` is
represented as a ConfigMap (`configmap.yaml`), and [Stakater
Reloader](https://github.com/stakater/Reloader) restarts the pod whenever that
ConfigMap changes — edit the config in git, sync, and the new settings apply.

## Prerequisites

- [Reloader](https://github.com/stakater/Reloader) installed in the cluster
  (`helm install reloader stakater/reloader`). Without it the manifests still
  work, but config changes only apply on the next manual rollout
  (`kubectl rollout restart deployment/remux`).
- A default StorageClass, or set `storageClassName` in `pvc.yaml`.

## Deploy

```sh
kubectl apply -k deploy/k8s
```

or point Argo CD / Flux at this directory.

## How configuration flows

1. `configmap.yaml` carries `config.toml`, mounted at `/etc/remux/config.toml`.
2. The `CONFIG=/etc/remux/config` env var tells the server to load it
   (the `config` crate resolves the `.toml` extension).
3. Environment variables **override** the file. The container image bakes
   `DATABASE_URL`, `DATA_DIR` and `TORRENT_DATA_DIR` (all pointing at `/data`,
   matching the PVC mount), so change those via `env` in `deployment.yaml`,
   not in the ConfigMap.
4. Secrets and API tokens should come from a Kubernetes Secret via `envFrom`
   (commented stub in `deployment.yaml`) — env keys map onto `Config` fields
   by lowercased name (e.g. `REMUXDB_URL` → `remuxdb_url`). Keep them out of
   the git-tracked ConfigMap.

## Constraints to be aware of

- **Do not scale above 1 replica.** The database is SQLite on the PVC;
  `strategy: Recreate` ensures the old pod releases it before a new one starts.
- **Probes** use the unauthenticated `GET /system/ping` endpoint.
- **Torrent peers**: outbound-only operation needs nothing. For incoming
  peers, uncomment the `hostPort` on the `torrent-peer` container port (or
  expose 6881 via a LoadBalancer) so the announced port is actually reachable.
- **DHT** can be turned off with `disable_dht = true` in the ConfigMap if the
  cluster network is restricted.
