# qrich

A richer `qstat` for ABCI-Q. Reads `qstat -f -F json` and renders what the plain
table leaves out: GPU count, a walltime progress bar, point burn against the
group's balance, the node the job landed on, and the log path to tail.

```
 ABCI-Q qjcm · <user> · 13 running · 3 held · 52 GPU · 68 pt/h · 21 Aug 14:03

 ID          NAME                     TYPE       GPU WALLTIME             % USED/REQ       GROUP      POINTS       NODE
 190662   ●R train_csj_sbint06_card… rt_QF        4 ▕███████████▏░░▏   80% 19:08/24:00    <group-a>  96/120       qh349
 190524   ●R train.sh                 rt_QF×2      8 ▕███████░░░░░░░▏   50% 24:04/48:00    <group-b>  241/480      qh352+2
 191417   ●R xrs_xl16_01_06           rt_QG        1 ▕▏░░░░░░░░░░░░░▏    1% 0:15/24:00     <group-b>  1/48         qh339
 190746   ●H eval_csj.sh              rt_QG        1   held 15:26         - -/4:00         <group-a>  0/8          -

 1,108 pt spent so far · 2,800 pt reserved by these jobs
 <group-a>    ▕███████████▍▏  94% 16,939 left of 300,000
 <group-b>    ▕██████████▍░▏  87% 8,032 left of 60,000
```

Each job shows the group it is **charged** to (`#PBS -W group_list=`), colour-
coded so the same group reads the same in the table and in the balance lines
underneath. When every listed job charges one group there is no column — the
header says `charged <group>` once and the space goes to the job name instead.

## Why not just `qstat`

- **`qstat -u $USER` does not work on ABCI-Q.** The `qstat` first in `PATH` is a
  site wrapper that rejects `-u`. qrich never passes `-u` — it asks for all jobs
  and filters by owner itself, so it works with the wrapper *and* the stock
  `/opt/pbs/bin/qstat`.
- Plain `qstat` shows `Time Use`, which is **CPU time**, not walltime. On a
  192-core node that number races ahead of the wall clock and tells you nothing
  about how close the job is to being killed by its limit. qrich shows walltime
  used against walltime requested, and colours the bar red past 90%.
- Points are invisible in `qstat`. qrich prices each job from its resource type
  and shows the group's remaining balance from `show_point`.

## Build and install

```bash
cargo build --release                          # needs the rust toolchain
cargo install --path . --root ~/.local         # -> ~/.local/bin/qrich
```

The login node has `rust/1.93.1` as a module if you have no toolchain of your
own: `module load rust/1.93.1`. The only dependency is `serde_json`.

## Usage

```
qrich                     your jobs, compact table
qrich -l                  one detailed card per job (usage, points, log, submit args)
qrich -c                  cluster-wide capacity instead of jobs (see below)
qrich -w                  refresh every 10s; -w 30 for a slower tick
qrich -x                  include recently finished jobs
qrich -a                  every user's jobs
qrich -u <user>           someone else's jobs
qrich <jobid>...          specific jobs, whoever owns them
qrich --no-points         skip the show_point lookup
qrich --ascii             plain ASCII bars
qrich --color never       no ANSI colour (also honours NO_COLOR and non-TTY)
qrich --width 100         override terminal width detection
```

## Cluster capacity (`-c`)

```
 ABCI-Q qjcm · cluster · 21 Aug 21:47
 496 nodes reachable by abciq · 5 offline/down excluded

 GPU   ▕██▍░░░░░░░░░░░░░░░░░▏  12% 1,725 of 1,964 free
 CPU   ▕██▍░░░░░░░░░░░░░░░░░▏  12% 82,976 of 94,272 cores free
 NODE  ▕██▌░░░░░░░░░░░░░░░░░▏  12% 430 idle · 61 in use

 fits now    rt_QF=100 (430 idle, per-user cap 100) · rt_QG 1,725 GPUs · rt_QC 82,976 cores
 limits      100 rt_QF nodes running per user · 300 nodes running cluster-wide
 jobs        68 running · 0 queued · 22 held
 next window 7 Sep 09:30 → 10:00 · GC_pre_1 · 505 nodes · in 16d 11h
```

Node data comes from the **stock** `/opt/pbs/bin/pbsnodes` — the `/usr/local/bin`
wrapper refuses to run on the login node. Two things make a raw node count wrong,
and both are handled:

- **Qlist.** A node only runs your job if its `resources_available.Qlist`
  contains the queue you submit to, so nodes dedicated elsewhere are excluded
  (496 of 505 today).
- **Per-vnode GPUs.** The server's `resources_assigned.ngpus` reads 0 because
  GPUs are tracked per vnode; the per-node sum is used instead. It cross-checks
  exactly against the server's `resources_assigned.ncpus`.

`fits now` is capped by the queue's per-user limit (`max_run_res.spot_rt_QF`), so
it answers "what would start for *me*", not "what is idle". `next window` is the
soonest root reservation from `pbs_rstat` — PBS will not start a job whose
walltime runs into it, which is the usual reason a long job sits queued while
nodes look free.

## Point rates

Figures use the 特別支援課題利用 (Special Support) rate card — rt_QF 5, rt_QG 2,
rt_QC 1, rt_QD/QS/QA 10 points per node-hour, multiplied by node count. Other
usage categories have their own card:

```bash
export ABCIQ_POINT_RATES="rt_QF=7,rt_QG=3"
```

Spent is `rate × elapsed`, reserved is `rate × requested walltime` — the amount
PBS holds up-front. Both are estimates from the rate card, not a billing record;
`show_point` remains the authority.

## Layout

| File | Role |
|---|---|
| `src/main.rs` | argument parsing, job filtering/sorting, watch loop |
| `src/pbs.rs` | running `qstat`/`show_point`, JSON → `Job`, point rates |
| `src/render.rs` | responsive table, long cards, footer warnings |
| `src/fmt.rs` | PBS durations, sizes, timestamps, bars, padding |
