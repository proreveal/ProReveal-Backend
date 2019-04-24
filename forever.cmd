:LOOP 
spark-submit main.py &
timeout /T 1 /NOBREAK 
goto :LOOP
