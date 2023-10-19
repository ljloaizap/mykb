# Command Interface Line

## File system

Get total free disk space using text processing tool: **`awk`**
```sh
df -h / | awk 'NR==2 {print $4}'
```

## Output manipulation

Use **`jq`** tool to process _json_ input
```sh
echo '{"name": "John", "age": 30, "city": "New York"}' | jq -c '.city'
```