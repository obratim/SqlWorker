#! /bin/bash
hg parent --template '{rev}.{ifeq("{files|count}",0,0,1)}'

