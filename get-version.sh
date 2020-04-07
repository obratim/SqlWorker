#! /bin/bash
echo $(($(hg ident -n)"-0"))"."$(hg status | if (($(grep ^M -c) > 0)); then echo 1; else echo 0; fi)
