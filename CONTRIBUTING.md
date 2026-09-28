# Contributing

Bug reports and pull requests are welcome. Keep protocol changes backward-aware,
update `docs/protocol.md`, and add tests for malformed input and compatibility.

Run `colcon test --packages-select rfd_bridge` before submitting changes. New
Python code should pass flake8 and must keep all queues and receive allocations
bounded.
