# qrich

A richer `qstat` for ABCI-Q, plus **qlog**, a navigator for the jobs' logs
(one install ships both binaries). Reads `qstat -f -F json` and renders what the plain
table leaves out: GPU count, a walltime progress bar, point burn against the
group's balance, the node the job landed on, and the log path to tail.

```
 ABCI-Q qjcm · <user> · 13 running · 3 held · 52 GPU · 68 pt/h · 21 Aug 14:03

 ID          NAME                     TYPE       GPU WALLTIME             % USED/REQ       GROUP      POINTS       NODE
 190662   ●R train_large_4gpu.sh      rt_QF        4 ▕███████████▏░░▏   80% 19:08/24:00    <group-a>  96/120       qh349
 190524   ●R train_multinode.sh       rt_QF×2      8 ▕███████░░░░░░░▏   50% 24:04/48:00    <group-b>  241/480      qh352+2
 191417   ●R sweep_lr_01_06           rt_QG        1 ▕▏░░░░░░░░░░░░░▏    1% 0:15/24:00     <group-b>  1/48         qh339
 190746   ●H eval.sh                  rt_QG        1   held 15:26         - -/4:00         <group-a>  0/8          -

 1,108 pt spent so far · 2,800 pt reserved by these jobs
 <group-a>    ▕███████████▍▏  94% 17,000 left of 300,000
 <group-b>    ▕██████████▍░▏  87% 8,000 left of 60,000
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

## Install

```bash
cargo install --git https://github.com/Wataru-Nakata/qrich    # -> ~/.cargo/bin/qrich
```

Or from a clone:

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

## qlog — stream and search job logs

PBS scatters logs across submit directories, and the filename embeds a job id
you have to remember. qlog reads each job's own `Output_Path` / `Error_Path`
attribute instead, so logs are addressed by job id, never by path.

```
$ qlog                      # index; on a terminal j/k+Enter picks a job to open
   ID     S  NAME                      SIZE   WRITE PATH
 1 193768 R  train_a                 36 KiB     14h …/train_a.o193768
 2 194327 R  train_b                5.5 MiB      0s …/train_b.o194327
 3 194367 R  grpo_train.sh            3 KiB     59s …/grpo_train.sh.o194367

$ qlog -f                   # full-screen live view, one page per job
 ● 193768 vrvq_after150 — running · rt_QF · 4 GPU · wall 34% (16:08/48:00) · 81/240 pt · qh391
 [rank1] W0827 11:58:47 torch/_dynamo/convert_frame.py ...
 Loading weights: 100%|██████████| 552/552 [00:00, 3105.86it/s]
 iter    10  reward mean   -4.33 dB   KL/step  1.2 (14.2s/iter)
 …log fills the screen…
 a:all  [1:193768 vrvq_…]  2:194232* orig_…  3:194327 train…   j/k page · u/d scroll · q quit

$ qlog -g "CUDA out of memory" -x -C 2     # search all logs, incl. finished
 194201:8123: torch.cuda.OutOfMemoryError: CUDA out of memory. …
 1 matching line in 1 of 14 logs
```

- **Bare `qlog` on a terminal is a picker**: `j`/`k` moves the cursor over the
  index, `Enter` opens that job's page, `1`–`9` opens one directly, `f` opens
  the "all" page. Piped, it stays a plain table.
- **`-f` is a full-screen paged view** (alternate screen, like `less` — your
  shell scrollback comes back untouched on quit). Every job is a page:
  a title bar in the job's colour showing state, resource type, GPUs,
  walltime %, points and node, the log filling the screen, and a **tab bar**
  at the bottom mapping keys to jobs. Switching pages (`1`–`9`, `j`/`k`,
  `a` for the all-jobs page) swaps the whole screen to that job's buffered
  history — each page keeps the last few thousand lines. A `*` marks tabs
  whose job printed while hidden.
- **Scroll inside a page**: `u`/`d` half-page up/down, `g` oldest, `G` back to
  tailing. While scrolled, the title bar counts new lines arriving below.
- Piped, `-f` degrades to the plain `[jobid]`-prefixed stream, so
  `qlog -f | grep loss` and `qlog -f <jobid> > out.txt` still work.
  `qlog -f -g PATTERN` streams only matching lines in either form.
- **`\r` progress bars are condensed**: a bar that rewrites its line without a
  newline (tqdm, Lightning) surfaces as a snapshot of its current state every
  few seconds instead of flooding or stalling the stream.
- **`-g` searches** every log with plain substring matching (`-i` for
  ASCII-case-insensitive, `-C N` for context). Exits 0 on a hit, 1 on none,
  grep-style.
- Logs that don't exist yet are announced and picked up the moment the job
  starts writing — submit, run `qlog -f`, and wait. A running job with no log
  usually lacks `#PBS -k oed` (output stays spooled until the job ends).
- Jobs without `-j oe` get their separate stderr as an extra `<jobid>.e` entry.

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

## Scope

Written for **ABCI-Q** (AIST). The generic half — walltime bars, job cards,
resource/usage parsing — is plain PBS Professional and should work on any PBS
cluster. The ABCI-Q specifics are: the `rt_Q*` resource types, the point rate
card, the `abciq` queue used to filter the node pool in `-c`, and the
`/usr/local/bin` command wrappers. Adapting it elsewhere mostly means changing
those.

## Layout

| File | Role |
|---|---|
| `src/main.rs` | qrich: argument parsing, job filtering/sorting, watch loop |
| `src/bin/qlog.rs` | qlog: log index, multiplexed follow, search |
| `src/pbs.rs` | running `qstat`/`show_point`, JSON → `Job`, point rates |
| `src/render.rs` | responsive table, long cards, footer warnings |
| `src/fmt.rs` | PBS durations, sizes, timestamps, bars, padding |

## License

MIT — see [LICENSE](LICENSE).
