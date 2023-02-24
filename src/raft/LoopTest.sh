TestType=3
#1 background 2A
if [ $TestType = 1 ]
then
  go test -run 2A > log &
fi

#2 Test2A
if [ $TestType = 2 ]
then
  Cnt=0
  for i in {1..100}
  do
    res=$(go test -run -2A | grep ok | wc -l)
    if ((res==1));then
      ((Cnt=Cnt+1))
    fi
  done
  echo $Cnt
fi

#3 Test2B
if [ $TestType = 3 ]
then
  Cnt=0
  for i in {1..10000}
  do
    res=$(go test -run -2B | grep ok | wc -l)
    if ((res==1));then
      ((Cnt=Cnt+1))
    fi
  done
  echo $Cnt
fi
