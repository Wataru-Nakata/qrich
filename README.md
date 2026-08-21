# qrich

A richer `qstat` for ABCI-Q. Reads `qstat -f -F json` and renders what the plain
table leaves out: GPU count, a walltime progress bar, point burn against the
group's balance, the node the job landed on, and the log path to tail.

```
 ABCI-Q qjcm · <user> · 13 running · 2 held · 48 GPU · 64 pt/h · 21 Aug 12:20

 ID          NAME                    TYPE       GPU WALLTIME             % USED/REQ       POINTS       NODE
 190662   ●R train_csj_sbint06_card… rt_QF        4 ▕██████████▏░░░▏   73% 17:25/24:00    87/120       qh349
 190524   ●R train.sh                rt_QF×2      8 ▕██████▌░░░░░░░▏   46% 22:16/48:00    223/480      qh352+2
 191417   ●R xrs_xl16_01_06          rt_QG        1 ▕▏░░░░░░░░░░░░░▏    1% 0:15/24:00     1/48         qh339
 190746   ●H eval_csj.sh             rt_QG        1   held 15:26         - -/4:00         0/8          -

 1,108 pt spent so far · 2,800 pt reserved by these jobs
 <group>      ▕███████████▍▏  94% 16,939 left of 300,000
```

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
