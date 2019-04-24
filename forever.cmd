:LOOP 
spark-submit main.py
timeout /T 1 /NOBREAK 
if not ErrorLevel 1 goto :LOOP
