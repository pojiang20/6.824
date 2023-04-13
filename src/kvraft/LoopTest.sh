TestType=1
TestRound=1

#1 TestFunc
if [ $TestType = 1 ]
then
  testFunc="TestConcurrent3A"
  echo "${testFunc} start $(date +%Y-%m-%d" "%H:%M:%S)"
  Cnt=0
  for i in $(seq 1 $TestRound)
    do
      go test -v -run ${testFunc} > log
      res=$(cat log | grep Passed | wc -l)
      if ((res==1));then
        ((Cnt=Cnt+1))
      else
        mv log "3Aerr_${testFunc}_${Cnt}"
      fi
    done
    echo "Passing rate: $Cnt/$TestRound"
    echo "Finish $(date +%Y-%m-%d" "%H:%M:%S)"
fi

#1 background 3A
if [ $TestType = 2 ]
then
  go test -run 3A > log
fi

#2 Test2A
echo "start $(date +%Y-%m-%d" "%H:%M:%S)"
if [ $TestType = 3 ]
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