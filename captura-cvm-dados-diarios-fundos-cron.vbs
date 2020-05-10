DIM objShell
set objShell=wscript.createObject("wscript.shell")
iReturn=objShell.Run("cron.bat", 0, TRUE)