# Command Interface Line

## File system

Get total free disk space using text processing tool: `awk`
```sh
df -h / | awk 'NR==2 {print $4}'
```