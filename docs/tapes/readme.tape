# VHS tape showcasing common workflows for the README
Output docs/images/readme.gif

Set Shell "bash"
Set FontFamily "FiraCode Nerd Font"
Set FontSize 16
Set Width 1000
Set Height 800
Set LetterSpacing 0
Set LineHeight 1.2
Set Padding 20
Set Theme "Dracula"
Set MarginFill "#fe00fe"
Set WindowBar Colorful
Set Margin 20
Set BorderRadius 10

# 1. One-shot: download + decode + gzip
Type "# one-shot: download, decode, and compress"
Enter
Type "sracha get SRR28588231 -f -O /tmp"
Enter
Sleep 10s

Type "clear"
Enter

# 2. Two-step: fetch then convert
Type "# two-step: fetch SRA, then convert to FASTQ"
Enter
Type "sracha fetch SRR28588231 -f --no-progress -O /tmp"
Enter
Sleep 6s
Type "sracha fastq /tmp/SRR28588231.sra -f -O /tmp"
Enter
Sleep 4s

Type "clear"
Enter

# 3. Accession metadata
Type "# run metadata"
Enter
Type "sracha info SRR28588231"
Enter
Sleep 4s

Type "clear"
Enter

# 4. Project metadata (resolves to runs via EUtils)
Type "# project metadata (resolves to runs via EUtils)"
Enter
Type "sracha info PRJNA675068"
Enter
Sleep 7s
