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

## Text viewer

`{command} | less`, and later `/` to search for a string, e.g.:
```
python --help | less` and then, `/ {search phrase}
```

## Network

Check if a service is available in a specific port with `telnet`.
```
telnet <ip> <port_number>
telnet 127.0.1.1 5432
```

To see if a service is running and listening on a specific port, use `netstat`.
```
netstat -tuln | grep 5432
```

Using `ip`, see network configuration  |  Get ip
```
apt-get install iproute2
ip a
hostname -i
```
