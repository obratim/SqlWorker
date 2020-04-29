#! /bin/bash
echo -n $(hg parent --template '{rev}').$(echo -n $(hg id -n) | sed -e 's/[0-9]*//' -e 's/\+/1/' -e 's/^$/0/')
