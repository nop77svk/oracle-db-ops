# Real-time process monitoring (notes)

Prepare-phase questions:
 * Used hosts, databases, instances (in RAC)?
 * When does the process run?
 * How do I distinguish the process from other processes? (Logging, instrumentation,...)
 * Is it pure PLSQL or is there outside-of-DB tech stack involved?
 * Is it serial or multi-threaded?
 * Are there parallel operations? What DOP is used? Is DOP configurable?
 * Was there any pre-production/testing run involved, to be inspected?

Prepare-phase actions:
 * Find corresponding monitoring dashboards (if available).
 * Prepare monitoring queries (if needed).

Monitoring general:
 * CPU usage, disk I/O, network I/O (host-related) [ASH, dyn.perf.views]
 * disk space - tablespaces usage, FRA usage (disk-related) [ASH, dyn.perf.views]
 * concurrent processes [ASH, dyn.perf.views]

Monitoring specific:
 * blocking sessions
 * locking issues
 * bad exec plans
 * LIOs per execution/row-fetch too high [v$sql]
 * missing indexes over FKs [enq: TX contention]

Post-monitoring:
 * local fuckups [AWR report]
 * globally correlated fuckups [historical dashboards]
