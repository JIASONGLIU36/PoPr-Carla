@echo off
wsl -d docker-desktop sh -c "sysctl -w net.core.rmem_max=16000000"
wsl -d docker-desktop sh -c "sysctl -w net.core.wmem_max=16000000"
pause