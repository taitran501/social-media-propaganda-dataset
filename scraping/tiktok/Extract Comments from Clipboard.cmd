@ECHO OFF
REM filepath: c:\Users\trant\Documents\Test\scrape\TikTokCommentScraper\Extract Simple Comments.cmd
"%~dp0.\python38\python.exe" "%~dp0.\src\ScrapeTikTokComments.py"

FOR /F "DELIMS=#" %%E IN ('"PROMPT #$E# & FOR %%E IN (1) DO REM"') DO (SET "\E=%%E")
ECHO %\e%[32m[*]%\e%[0m Đã xuất file theo định dạng mới. Nhấn phím bất kỳ để đóng.
>NUL PAUSE