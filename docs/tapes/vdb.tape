# VHS tape for sracha vdb command
Output docs/images/vdb.gif

Set Shell "bash"
Set FontFamily "FiraCode Nerd Font"
Set FontSize 16
Set Width 1000
Set Height 700
Set LetterSpacing 0
Set LineHeight 1.2
Set Padding 20
Set Theme "Dracula"
Set MarginFill "#fe00fe"
Set WindowBar Colorful
Set Margin 20
Set BorderRadius 10

# Pre-fetch the fixture off-camera so the demo is self-contained
Hide
Type "sracha fetch SRR28588231 -f --no-progress -O /tmp"
Enter
Sleep 8s
Type "clear"
Enter
Sleep 500ms
Show

Type "sracha vdb info /tmp/SRR28588231.sra"
Enter
Sleep 2s
Type "sracha vdb tables /tmp/SRR28588231.sra"
Enter
Sleep 1s
Type "sracha vdb columns /tmp/SRR28588231.sra"
Enter
Sleep 3s
