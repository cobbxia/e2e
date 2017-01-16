#!/usr/bin/env sh

# 1. stop the compile driver
ps -ef | grep CompileDriver | awk '{print $2}' | xargs -i kill {}

# 2. stop the 3lianpao.sh
ps -ef | grep 3lianpao | awk '{print $2}' | xargs -i kill {}

# 3. kill java
pkill java
