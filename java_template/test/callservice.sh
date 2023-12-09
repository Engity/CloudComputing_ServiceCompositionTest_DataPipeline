#!/bin/bash
bucketName="test.project.tcss462.ngt"
switchBoardFunction="projectTestProcessCSV"
transformFunctionName="switchBoard"
loadFunctionName="processSalesData"

echo "Which operation would you like to perform?"
echo "1. Transform"
echo "2. Load"
echo "3. Query"
echo "4. TL"
echo "5. LQ"
echo "6. TLQ"
read -p "Enter your option " option1

case $option1 in
    1) 
    operationType=1
    echo "You chose to perform Transform"
    ;;

    2) 
    operationType=2
    echo "You chose to perform Load"
    ;;

    3) 
    operationType=3
    echo "You chose to perform Query"
    ;;

    4) 
    operationType=4
    echo "You chose to perform TL"
    ;;

    5) 
    operationType=5
    echo "You chose to perform LQ"
    ;;

    6) 
    operationType=6
    echo "You chose to perform TLQ"
    ;;

    *)
    echo "Entered option is invalid, we will use the default operation Transform"
    operationType=1
    ;;

esac

if [ "$operationType" = 1 ] || [ "$operationType" = 4 ] || [ "$operationType" = 6 ]; then
    echo "Which file would you like to use?"
    echo "1. 100"
    echo "2. 1000"
    echo "3. 10000"
    echo "4. 50000"
    echo "5. 100000"
    echo "6. 500000"
    echo "7. 1000000"
    echo "8. 1500000"

    read -p "Enter your option " option

    case $option in
        1) 
        option=100
        ;;

        2) 
        option=1000
        ;;

        3) 
        option=10000
        ;;

        4) 
        option=50000
        ;;

        5) 
        option=100000
        ;;

        6) 
        option=500000
        ;;

        7) 
        option=1000000
        ;;

        8) 
        option=1500000
        ;;

        *)
        echo "Entered option is invalid, we will use the default csv file with 100 records"
        option=100
        ;;

    esac
fi


# JSON object to pass to Lambda Function
fileName=${option}"SalesRecords.csv"

echo You choose the file ${fileName} 
jsonT={"\"bucketname\"":\"${bucketName}\"","\"filename\"":\"${fileName}\"",""\"operationtype\"":${operationType}"}
# echo $jsonT


echo "Invoking Process CSV Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name $switchBoardFunction --region us-east-2 --payload $jsonT --cli-connect-timeout 900 --cli-read-timeout 900 /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq