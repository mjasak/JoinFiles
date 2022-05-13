#!/bin/bash
pip3 install -r ./requirements.txt
function join(){
if [ -z $4 ]
then
python3 join_func.py --file1 $1 --file2 $2 --col $3
else
python3 join_func.py --file1 $1 --file2 $2 --col $3 --type $4
fi
}
