- Run

	    cd cmd
	    go build && ./cmd

- Test with official mysql client

	    mysql -h 127.0.0.1 -P 4000


- Test with go mysql driver

	    export MYSQL_TEST_ADDR=127.0.0.1:4000
	    go test github.com/go-sql-driver/mysql

If the mysql result is used (passed as argument in function `NewComboDriver`), the test will pass, if ql result is different, it is logged as warning.
