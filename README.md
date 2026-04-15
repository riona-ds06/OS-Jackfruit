# Multi-Container Runtime

Team:
**Riona Jeena Dsouza - PES2UG24CS654**
**`Ramya - PES2UG24CS653**

## **Project Summary**

This project involves building a lightweight Linux container runtime in C with a long-running parent supervisor and a kernel-space memory monitor. The container runtime must manage multiple containers at once, coordinate concurrent logging safely, expose a small supervisor CLI, and include controlled experiments related to Linux scheduling.

The project has two integrated parts:

User-Space Runtime + Supervisor (engine.c)
Launches and manages multiple isolated containers, maintains metadata for each container, accepts CLI commands, captures container output through a bounded-buffer logging system, and handles container lifecycle signals correctly.
Kernel-Space Monitor (monitor.c)
Implements a Linux Kernel Module (LKM) that tracks container processes, enforces soft and hard memory limits, and integrates with the user-space runtime through ioctl.

---

## **Repository Layout**

*`engine.c: user-space supervisor, CLI, container launch path, logging pipeline`
*`monitor.c: Linux kernel module for container memory monitoring`
*`monitor_ioctl.h: shared ioctl definitions`
*`cpu_hog.c: CPU-bound workload`
*`io_pulse.c: I/O-oriented workload`
*`memory_hog.c: memory pressure workload`
*`Makefile: build targets for user-space and kernel-space components`

## **Build, Load, and Run Instructions**

These steps assume an Ubuntu 22.04 or 24.04 VM with Secure Boot disabled.

**1. Install dependencies**
```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

**2. Environment precheck**
```bash
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
```

**3. Prepare the Alpine root filesystem**
In cd ~/OS-Jackfruit:

```bash
mkdir -p rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
cp -a ./rootfs-base ./rootfs-gamma
```

**4. Build the project**

```bash
cd boilerplate
make clean
make
make ci
```

**5. Load the kernel module**

```bash
cd boilerplate
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

**6. Copy workloads into the container rootfs**
From the repository root:

```bash
cp ./boilerplate/memory_hog ./rootfs-alpha/
cp ./boilerplate/cpu_hog ./rootfs-beta/
cp ./boilerplate/io_pulse ./rootfs-gamma/
cp boilerplate/cpu_hog rootfs-cpu-io/
cp boilerplate/io_pulse rootfs-io-io/
```

**7. Start the supervisor**

From boilerplate/:

```bash
cd OS-Jackfruit/boilerplate
sudo insmod monitor.ko
sudo ./engine supervisor ../rootfs-base
```
The supervisor creates a UNIX domain control socket at /tmp/mini_runtime.sock and stores per-container logs under boilerplate/logs/.

**8. Multi-container supervision and metadata**

```bash
sudo ./engine start alpha ../rootfs-beta "/bin/sleep 30"
sudo ./engine start beta ../rootfs-gamma "/bin/sleep 30"
sudo ./engine ps
```
These commands were used to demonstrate one supervisor managing multiple containers concurrently and to capture the ps metadata table.

**9. Logging pipeline demo**

```bash
sudo ./engine start logger ../rootfs-gamma "/io_pulse 10 200"
sleep 3
sudo ./engine logs logger
ls -l logs/
```
This run was used to show that container output was captured through the bounded-buffer logging pipeline and written to a persistent per-container log file.

**10. CLI and IPC demo**

```bash
sudo ./engine ps
```
This command was used while the supervisor was already running in another terminal so that the control-plane request/response behavior could be shown.

**11. Soft-limit warning demo**

```bash
sudo ./engine start memsoft ../rootfs-alpha "/memory_hog 8 200" --soft-mib 16 --hard-mib 64
sleep 3
sudo dmesg | tail -n 20
```
This sequence was used to capture the soft-limit warning emitted by the kernel monitor after the container crossed the configured RSS threshold.

**12. Hard-limit enforcement demo**

```bash
sudo ./engine start memkill ../rootfs-alpha "/memory_hog 8 200" --soft-mib 16 --hard-mib 24
sleep 5
sudo dmesg | tail -n 20
sudo ./engine ps
```
This sequence was used to show hard-limit enforcement in dmesg together with the matching user-space metadata attribution of hard_limit_killed.

**13. Scheduling experiment demo**

```bash
sudo ./engine start schedcpu ../rootfs-cpu-io "/cpu_hog 6" --nice 10
sudo ./engine start schedio ../rootfs-io-io "/io_pulse 20 200" --nice 0
sleep 7
sudo ./engine ps
sudo ./engine logs schedcpu
sudo ./engine logs schedio
```
These commands were used to compare a CPU-bound and an I/O-bound workload running under different scheduling settings and to capture both the metadata view and their logs.

**14. Teardown demo**

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
sudo ./engine stop logger
sudo ./engine stop schedcpu
sudo ./engine stop schedio
ps -eo pid,ppid,stat,cmd | grep -E "(\\./engine|cpu_hog|io_pulse|memory_hog)" | grep -v grep
sudo rmmod monitor
sudo dmesg | tail -n 20
```
This final sequence was used to demonstrate that the tracked containers had either already exited or were stopped cleanly, that no project workload processes remained, and that the kernel monitor could be unloaded successfully.



## **Demo With Screenshots**
---

**1. Multi-container supervision**
Two or more containers running under one supervisor process
<img width="1050" height="152" alt="Screenshot from 2026-04-15 09-41-13" src="https://github.com/user-attachments/assets/62ece944-92f0-4011-b435-fd9bb264b60f" />

**2. Metadata tracking**
Output of the ps command showing tracked container metadata
<img width="1205" height="153" alt="Screenshot from 2026-04-15 09-42-17" src="https://github.com/user-attachments/assets/4ac79de9-bf12-4c96-ae96-095a235ff5ac" />

**3. Bounded-buffer logging**
Log file contents captured through the logging pipeline, and evidence of the pipeline operating (e.g., producer/consumer activity)
<img width="1211" height="452" alt="Screenshot from 2026-04-15 09-58-29" src="https://github.com/user-attachments/assets/3317d1b0-43ed-4c54-87a3-b5ea7acf930c" />

**4. CLI and IPC**
A CLI command being issued and the supervisor responding, demonstrating the second IPC mechanism
<img width="1078" height="52" alt="Screenshot from 2026-04-15 09-59-15" src="https://github.com/user-attachments/assets/4d9784e7-88f6-4577-a11d-794352df4ef9" />
<img width="1217" height="186" alt="Screenshot from 2026-04-15 09-59-30" src="https://github.com/user-attachments/assets/80cd63bd-f82f-47b8-87db-91d1f1f22f7f" />

**5. Soft-limit warning**
dmesg or log output showing a soft-limit warning event for a container
<img width="1208" height="616" alt="Screenshot from 2026-04-15 10-04-18" src="https://github.com/user-attachments/assets/132ff0f8-f435-4f48-b8cd-29233417559b" />
<img width="1217" height="683" alt="Screenshot from 2026-04-15 10-05-13" src="https://github.com/user-attachments/assets/bf4eb468-12c5-418b-9530-c7b2cc2c7ae8" />

**6. Hard-limit enforcement**
dmesg or log output showing a container being killed after exceeding its hard limit, and the supervisor metadata reflecting the kill
<img width="1217" height="683" alt="Screenshot from 2026-04-15 10-06-51" src="https://github.com/user-attachments/assets/1bfcf464-3303-4bb8-bcc7-7d521b2836ab" />
<img width="1217" height="683" alt="Screenshot from 2026-04-15 10-07-02" src="https://github.com/user-attachments/assets/2e830e40-f8ed-4907-9ef1-307a15df2874" />
<img width="1212" height="313" alt="Screenshot from 2026-04-15 10-07-58" src="https://github.com/user-attachments/assets/b28df9ec-bbbe-4bdd-9404-c25fc1112914" />

**7. Scheduling experiment**
Terminal output or measurements from at least one scheduling experiment, with observable differences between configurations
<img width="1207" height="311" alt="Screenshot from 2026-04-15 11-36-31" src="https://github.com/user-attachments/assets/e1420a7a-d3fc-4620-a8c9-14eb838bcd7b" />
<img width="1197" height="680" alt="Screenshot from 2026-04-15 11-37-28" src="https://github.com/user-attachments/assets/2da6c252-7396-4dda-b5ea-ca90489796a0" />

**8. Clean teardown**
Evidence that containers are reaped, threads exit, and no zombies remain after shutdown (e.g., ps aux output, supervisor exit messages)
<img width="1213" height="208" alt="Screenshot from 2026-04-15 11-48-10" src="https://github.com/user-attachments/assets/7f1a6a69-1acd-4e6d-8569-2dad0f354005" />
<img width="1203" height="135" alt="Screenshot from 2026-04-15 11-48-28" src="https://github.com/user-attachments/assets/66b10dc0-f20d-44e0-984c-9507da98ebab" />
<img width="1213" height="680" alt="Screenshot from 2026-04-15 11-48-58" src="https://github.com/user-attachments/assets/2cf45b22-1465-4454-8e31-f90f3e611f80" />


## **Engineering Analysis**
---

1. Isolation Mechanisms
The runtime isolates each container using PID, UTS, and mount namespaces created through clone(). PID namespaces give the container its own process numbering view, so processes inside the container see their own PID hierarchy instead of the host's. UTS namespaces isolate hostname state, which lets the runtime set a container-specific hostname. Mount namespaces isolate mount-table changes so that mounting /proc inside one container does not affect the host or other containers.

Filesystem isolation is implemented with chroot() into a per-container rootfs copy. This gives each running container a separate writable filesystem tree derived from the same Alpine base. The host kernel is still shared across all containers, so this is isolation at the namespace and filesystem-view level rather than full virtualization.

2. Supervisor and Process Lifecycle
A long-running supervisor is useful because container management is stateful. The supervisor keeps metadata for every container, owns the control socket, tracks log files, registers container PIDs with the kernel monitor, and makes sure children are reaped. Without a persistent parent, each CLI command would have no shared state and background container management would be much harder.

The runtime launches containers as children of the supervisor. The supervisor keeps host PIDs, start time, configured limits, exit information, and termination reason. A dedicated reaper path calls waitpid() so exited children do not remain as zombies. Signals flow through the supervisor, which allows the design to distinguish normal exit, manual stop, and hard-limit kill.

3. IPC, Threads, and Synchronization
The design uses two IPC mechanisms:

*`control-plane IPC: UNIX domain socket between CLI clients and the supervisor`
*`logging IPC: anonymous pipes from container stdout/stderr into the supervisor`
The logging path uses a bounded buffer protected by a mutex plus two condition variables. Producers block when the buffer is full, and the consumer blocks when the buffer is empty. This prevents busy-waiting and makes shutdown explicit. Shared container metadata is protected separately with a dedicated mutex and condition variable, because metadata access and log-buffer access are different critical sections with different waiting behavior.

Without synchronization, several races would occur:

*`two concurrent start commands could reuse the same container ID or rootfs`
*`producer and consumer threads could corrupt buffer indices`
*`ps, stop, and reaping could observe partially updated metadata`
*`run could miss the completion of its target container`

4. Memory Management and Enforcement
RSS measures resident set size: the amount of a process's memory that is currently resident in physical RAM. It does not directly measure total virtual memory size, swapped-out pages, or all kernel-side memory associated with the process. That makes it a useful but incomplete signal for memory pressure.

Soft and hard limits serve different policy goals. A soft limit is a warning threshold that helps surface abnormal growth without killing the process immediately. A hard limit is an enforcement threshold that protects the system from uncontrolled memory use. Enforcement belongs in kernel space because the kernel has authoritative access to process memory accounting and can reliably signal or kill the target process even if user-space monitoring is delayed, blocked, or compromised.

5. Scheduling Behavior
The scheduler experiments use the runtime as a platform for controlled comparison. By changing nice values and workload types, we can observe how Linux balances fairness, responsiveness, and throughput. CPU-bound workloads compete for time slices, so priority differences should affect observed progress and completion time. An I/O-bound workload usually yields frequently, so it can remain responsive even while CPU-heavy work is running. The exact numbers depend on VM load and CPU allocation, so the final report should explain the measured results from your own run rather than only theory.


## **Design Decisions and Tradeoffs**
---

1. Namespace isolation
*`Choice: clone() with PID, UTS, and mount namespaces plus chroot() into a per-container rootfs`
*`Tradeoff: chroot() is simpler than pivot_root() but offers weaker filesystem isolation semantics`
*`Justification: it keeps the implementation understandable while still meeting the project requirement for isolated root filesystems and working /proc`

2.Supervisor architecture
*`Choice: one long-running supervisor process with a control socket and per-request handler threads`
*`Tradeoff: thread-based concurrency adds synchronization complexity`
*`Justification: it allows run to wait for a container without blocking the entire supervisor from serving other CLI requests`

3.IPC and logging
*`Choice: UNIX domain socket for control requests and pipes for log capture`
*`Tradeoff: two IPC mechanisms increase implementation surface area`
*`Justification: the project explicitly requires the control plane to be distinct from the logging plane, and this split matches their roles naturally`

4.Kernel monitor
*`Choice: linked-list tracking protected by a mutex, with timer-triggered work executed in workqueue context`
*`Tradeoff: this is more complex than doing everything directly in the timer callback`
*`Justification: workqueue context allows the monitor to use sleeping primitives safely while still checking memory periodically`

5.Scheduler experiments
*`Choice: CPU-bound, I/O-bound, and memory-growth workloads built as standalone helpers`
*`Tradeoff: these workloads are intentionally simple and are not perfect models of production programs`
*`Justification: they are predictable enough for classroom experiments and easy to reproduce in screenshots and analysis`


## **Scheduler Experiment Results**
---

Experiment 1 - CPU-bound vs CPU-bound with different priorities

| Container | Nice value | Observed CPU% | Weight Ratio (Approx.) |
| :--- | :--- | :--- | :--- |
| `cpua` | -10 | ~75-80% | 9544 |
| `cpub` | +10 | ~20-25% | 10 |

Analysis: The Completely Fair Scheduler (CFS) allocates timeslices based on vruntime (virtual runtime). Tasks with lower nice values (higher priority, like -10) have their vruntime increment at a fraction of the actual physical time. This mathematically tricks the CFS tree into scheduling them far more often. The ~4x difference in CPU share confirms that the high-priority container successfully dominated cycles, consistent with CFS weight theory.


Experiment 2 - CPU-bound vs I/O-bound

| Container | Workload | Observed behavior |
| :--- | :--- | :--- |
| `alpha` | `cpu_hog` | Consistently high CPU%, long scheduling intervals |
| `beta` | `io_pulse` | Low average CPU%, but gets CPU immediately when I/O completes |

Analysis: CFS tracks vruntime; processes that sleep accumulate less virtual runtime. When the I/O-bound container (beta) wakes up, it is scheduled ahead of the CPU-bound container because it has the lowest vruntime. This demonstrates the scheduler's built-in preference for interactive and I/O-bound workloads to balance throughput with responsiveness.





