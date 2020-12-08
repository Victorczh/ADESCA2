const { reduceRight } = require('async');
const { Pool, Client } = require('pg');
const connectionString = 'postgres://xuxdjfbq:rvTPoHRABVvZs9h0l6MUxZh1vL48MgFj@john.db.elephantsql.com:5432/xuxdjfbq';
// Connect to ElephantSQL 
const pool = new Pool({
    connectionString,
    max: 4,
    statement_timeout: 10000
});

pool.connect();

pool.on('error', (err, client) => {
    console.error('Unexpected error on idle client', err)
    process.exit(-1)
})

function getDatabasePool() {
    return pool;
}
// For Reset API
function resetTables(callback) {
    const pool = getDatabasePool();
    const sql = `DELETE FROM customerqueuenumber;`;
    pool.query(sql, function (err, res) {
        if (err) {
            return callback(err, null);
        }
        else {
            const sql = `DELETE FROM queue;`;
            pool.query(sql, function (err, res) {
                if (err) {
                    return callback(err, null);
                }
                else {
                    return callback(null, res);
                }
            });
        }
    });
}
// For Create Queue API
function createQueue(company_id, queue_id, callback) {
    const pool = getDatabasePool();
    const sql = 'INSERT INTO Queue(queue_id,current_queue_number, status, company_id)VALUES($1,$2,$3,$4)';
    pool.query(sql, [queue_id, 0, '0', company_id], function (err, res) {
        if (err) {
            return callback(err, null);
        }
        else {
            const sql = 'SELECT * FROM Queue WHERE queue_id = $1';
            pool.query(sql, [queue_id], function (err, res) {
                if (err) {
                    return callback(err, null);
                } else {
                    return callback(null, res.rows);
                }
            });
        }
    });
}
// For Update Queue API
function updateQueue(status, queue_id, callback) {
    const pool = getDatabasePool()
    const sql = 'UPDATE Queue SET status = $1 WHERE queue_id = $2';
    pool.query(sql, [status, queue_id], function (err, res) {
        if (err) {
            return callback(err, null);
        }
        else {
            const sql = 'SELECT queue_id FROM Queue WHERE queue_id = $1';
            pool.query(sql, [queue_id], function (err, res) {
                if (err) {
                    return callback(err, null);
                } else {
                    return callback(null, res.rows);
                }
            });
        }
    });
}
// For Server Available API
function serverAvailable(queue_id, callback) {
    var selectedResult;
    const pool = getDatabasePool();
    const sql = 'Select * from queue where queue_id = $1';
    pool.query(sql, [queue_id], function (err, res) {
        if (err) {
            return callback(err, null);
        }
        else if (res.rows.length == 0) {
            return callback(null, 'UNKNOWN_QUEUE');
        }
        else {
            const sql = 'SELECT C.customer_id FROM CustomerQueueNumber C, Queue Q where q.queue_id = $1 and Q.queue_id = C.queue_id and c.queue_number = q.current_queue_number+1';
            pool.query(sql, [queue_id], function (err, res) {
                if (err) {
                    return callback(err, null);
                }
                else if (res.rowCount == 0) {
                    return callback(null, []);
                }
                else {
                    selectedResult = parseInt(res.rows[0].customer_id);
                    const sql = 'UPDATE Queue SET current_queue_number = current_queue_number+1 WHERE queue_id = $1';
                    pool.query(sql, [queue_id], function (err, res) {
                        if (err) {
                            console.log('err in serverAvailable 2' + err);
                            return callback(err, null);
                        }
                        else {
                            return callback(null, selectedResult);
                        }
                    });
                }
            });
        }
    });
}
// For Check Queue API
function checkQueue(queue_id, customer_id, callback) {
    const pool = getDatabasePool();
    const sql = 'Select current_queue_number,status FROM Queue WHERE queue_id=$1';
    pool.query(sql, [queue_id], function (err, res) {
        if (err) {
            return callback(err, null);
        } else {
            if (res.rows == '') {
                return callback(null, 'NOEXIST');
            }
            else {
                const result1 = res.rows[0];
                const sql = 'Select count(queue_number) total FROM CustomerQueueNumber WHERE queue_id=$1';
                pool.query(sql, [queue_id], function (err, res) {
                    if (err) {
                        return callback(err, null);
                    } else {
                        const result2 = res.rows[0];
                        const result = Object.assign(result1, result2);
                        if (customer_id == undefined) {
                            return callback(null, result);
                        }
                        else {
                            const sql = 'Select queue_number FROM CustomerQueueNumber WHERE queue_id=$1 AND customer_id=$2';
                            pool.query(sql, [queue_id, customer_id], function (err, res) {
                                if (err) {
                                    return callback(err, null);
                                } else {
                                    const result3 = res.rows[0];
                                    const result2 = Object.assign(result, result3);
                                    return callback(null, result2);
                                }
                            });
                        }
                    }
                });
            }
        }
    });
}
// For Join Queue API
function joinQueue(customer_id, queue_id, callback) {
    const pool = getDatabasePool();
    // check if queue is active or not
    const sql = 'SELECT status,current_queue_number FROM Queue WHERE queue_id=$1';
    pool.query(sql, [queue_id], function (err, res) {
        if (err) {
            return callback(err, null);
        }
        else {
            // next queuenumber
            //queue does exist
            if (res.rows != '') {
                //queue active
                if (res.rows[0].status == true) {
                    //check if customer already joined queue
                    const sql = 'SELECT queue_number FROM CustomerQueueNumber WHERE queue_id=$1 AND customer_id=$2'//INSERT INTO CustomerQueueNumber(queue_number) VALUES (MAX(queue_number)+1) WHERE
                    pool.query(sql, [queue_id, customer_id], function (err, res) {
                        if (err) {
                            return callback(err, null);
                        }
                        //if customer not in queue
                        else if (res.rows == '') {
                            const sql = 'SELECT max(queue_number) FROM CustomerQueueNumber WHERE queue_id=$1'
                            pool.query(sql, [queue_id], function (err, res) {
                                if (err) {
                                    console.log(err);
                                    return callback(err, null);
                                }
                                else {
                                    const nextcurrentqueue = res.rows[0].max + 1;
                                    const sql = 'INSERT INTO CustomerQueueNumber(queue_number,customer_id,queue_id) VALUES ($1,$2,$3)';
                                    pool.query(sql, [nextcurrentqueue, customer_id, queue_id], function (err, res) {
                                        if (err) {
                                            return callback(err, null);
                                        }
                                        else {
                                            return callback(null, 'SUCCESS');
                                        }
                                    });
                                }
                            });
                        }
                        else {
                            return callback(null, 'EXIST');
                        }
                    });
                }
                else if (res.rows[0].status == false) {
                    // queue inactive return false
                    return callback(null, false);
                }
            }
            else {
                return callback(null, 'NOEXIST');
            }
        }
    });
}
// For Arrival Rate API
function arrivalRate(queue_id, from, duration, callback) {
    const pool = getDatabasePool();
    const sql = 'SELECT * FROM CustomerQueueNumber WHERE queue_id=$1';
    pool.query(sql, [queue_id], function (err, res) {
        if (err) {
            console.log(err);
            return callback(err, null);
        }
        else if (res.rows != '') {
            const sql = 'SELECT TO_TIMESTAMP(cast(time_joined as Text),$6) at time zone $4 at time zone $5 as timestamp,count(*) FROM CustomerQueueNumber WHERE queue_id=$1 and time_joined BETWEEN $2 AND $3 GROUP BY timestamp';
            pool.query(sql, [queue_id, from, duration, 'SGT', 'UTC', 'YYYY-MM-DD HH24:MI:SS'], function (err, res) {
                if (err) {
                    console.log(err);
                    return callback(err, null);
                }
                else if (res.rows != '') {
                    console.log(res.rows);
                    return callback(null, res.rows);
                } else {
                    //time range not found
                    return callback(null, 'TIMEERROR');
                }
            })
        }
        else {
            //queue id not exist
            return callback(null, 'NOEXIST');
        }
    });
}

function closeDatabaseConnections() {
    /**
     * return a promise that resolves when all connection to the database is successfully closed, and rejects if there was any error.
     */
    console.log("Run Teardown")
    return pool.end();
}

module.exports = {
    resetTables,
    closeDatabaseConnections,
    createQueue,
    updateQueue,
    serverAvailable,
    checkQueue,
    joinQueue,
    arrivalRate,
};