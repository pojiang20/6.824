TestType=4
TestRound=100
#1 background 2A
if [ $TestType = 1 ]
then
  go test -run 2A > log &
fi

#2 Test2A
echo "start $(date +%Y-%m-%d" "%H:%M:%S)"
if [ $TestType = 2 ]
then
  Cnt=0
  for i in $(seq 1 $TestRound)
  do
    go test -run 2A > log
    res=$(cat log | grep PASS | wc -l)
    if ((res==1));then
      ((Cnt=Cnt+1))
    else
      mv log "2Aerr_${Cnt}"
    fi
  done
  echo "Passing rate: $Cnt/$TestRound"
  echo "Finish $(date +%Y-%m-%d" "%H:%M:%S)"
fi

#3 background 2B
if [ $TestType = 3 ]
then
  go test -run 2B > log &
fi

#4 Test2B
if [ $TestType = 4 ]
then
  Cnt=0
  for i in $(seq 1 $TestRound)
  do
    go test -run 2B > log
    res=$(cat log | grep PASS | wc -l)
    if ((res==1));then
      ((Cnt=Cnt+1))
    else
      mv log "2Berr_${Cnt}"
    fi
  done
  echo "Passing rate: $Cnt/$TestRound"
  echo "Finish $(date +%Y-%m-%d" "%H:%M:%S)"
fi

#5 background 2C
if [ $TestType = 5 ]
then
  go test -run 2C > log &
fi

#6 Test2C
if [ $TestType = 6 ]
then
  Cnt=0
  for i in $(seq 1 $TestRound)
    do
      go test -run 2C > log
      res=$(cat log | grep PASS | wc -l)
      if ((res==1));then
        ((Cnt=Cnt+1))
      else
        mv log "2Cerr_${Cnt}"
      fi
    done
    echo "Passing rate: $Cnt/$TestRound"
    echo "Finish $(date +%Y-%m-%d" "%H:%M:%S)"
fi