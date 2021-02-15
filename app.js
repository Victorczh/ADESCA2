const express = require('express'); // DO NOT DELETE
const cors = require('cors');
const morgan = require('morgan');
const app = express(); // DO NOT DELETE

const database = require('./database');
const { json } = require('express');

app.use(morgan('dev'));
app.use(cors());

app.use(express.json());
const moment = require('moment');
const validator = require('./jsonValidation');
const { nextTick } = require('async');

/**
 * =====================================================================
 * ========================== CODE STARTS HERE =========================
 * =====================================================================
 */

/**
 * ========================== SETUP APP =========================
 */
app.use(morgan('dev'));
app.use(cors());

/**
 * JSON Body
 */
app.use(express.json());

/**
 * ========================== RESET API =========================
 */




/**
 * Reset API
 */
app.post('/reset', (req, res, next) => {
    database.resetTables(function (err, result) {
        if (!err) {
            console.log("Successfully reset");
            res.send('successs');
        }
        else {
            next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
        }
    })
});

/**
 * ========================== COMPANY =========================
 */

/**
 * Company: Create Queue
 */
app.post('/company/queue', function (req, res, next) {
    const company_id = req.body.company_id;
    var queueIdCaseSensitive = req.body.queue_id;
    // JSON validation
    var queueIdValidator = validator.isValid(queueIdCaseSensitive, validator.checkQueueId);
    var check10digits = validator.isValid(company_id, validator.check10digit);
    // If pass the JSON validation
    if (queueIdValidator && check10digits) {
        // Convert to uppercase
        var queue_id = queueIdCaseSensitive.toUpperCase();
        // Connect to database
        database.createQueue(company_id, queue_id, function (err, result) {
            if (!err) {
                res.status(201).send(result);
            }
            else {
                // Error code : 23505 (Unique Violation)
                if (err.code == '23505') {
                    next({ body: { error: "Queue Id '" + queue_id + "' already exists", code: 'QUEUE_EXISTS' }, status: 422 });
                } else {
                    next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
                }
            }
        });
    }// Validation failed - queue_id
    else if (!queueIdValidator) {
        next({ body: errors.INVALID_BODY_QUEUE.body, status: errors.INVALID_BODY_QUEUE.status });
    }// Validation failed - company_id
    else if (!check10digits) {
        next({ body: errors.INVALID_BODY_COMPANY.body, status: errors.INVALID_BODY_COMPANY.status });
    }
    else {
        next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
    }
})

/**
 * Company: Update Queue
 */
app.put('/company/queue', function (req, res, next) {
    var status = req.body.status;
    var queueIdCaseSensitive = req.query.queue_id;
    // JSON validation
    var queueIdValidator = validator.isValid(queueIdCaseSensitive, validator.checkQueueId);
    var statusValidator = validator.isValid(status, validator.checkStatus);
    // Validation passed
    if (queueIdValidator && statusValidator) {
        var queue_id = queueIdCaseSensitive.toUpperCase();
        // If the status is 'ACTIVATE', convert into 1
        if (req.body.status == 'ACTIVATE') {
            status = '1';
        } else {
            status = '0';
        }
        // Connect to database
        database.updateQueue(status, queue_id, function (err, result) {
            if (!err) {
                // Unknown queue(queue_id not found in database)
                if (result.length == 0) {
                    next({ body: { error: "Queue Id '" + queue_id + "' Not Found", code: 'UNKNOWN_QUEUE' }, status: 404 });
                } else {
                    // Success
                    res.status(200).send(result);
                }
            }// Unexpected Server Error
            else {
                next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
            }
        });
    }
    // Validation failed - queue_id
    else if (!queueIdValidator) {
        next({ body: errors.INVALID_QUERY_QUEUE.body, status: errors.INVALID_QUERY_QUEUE.status });
    }
    // Validation failed - status 
    else if (!statusValidator) {
        res.status(errors.INVALID_BODY_STATUS.status).send(errors.INVALID_BODY_STATUS.body);
    }
    else {
        next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
    }
});

/**
 * Company: Server Available
 */
app.put('/company/server', function (req, res, next) {
    var queueIdCaseSensitive = req.body.queue_id;
    // JSON Validation
    var queueIdValidator = validator.isValid(queueIdCaseSensitive, validator.checkQueueId);
    // Validation Passed
    if (queueIdValidator) {
        var queue_id = queueIdCaseSensitive.toUpperCase();
        // Connect to database
        database.serverAvailable(queue_id, function (err, result) {
            if (!err) {
                // Success but no customer
                if (result.length == 0) {
                    res.status(200).send({ customer_id: 0 });
                }
                // Non-existence Queue Id
                else if (result == 'UNKNOWN_QUEUE') {
                    next({ body: { error: "Queue Id '" + queue_id + "' Not Found", code: 'UNKNOWN_QUEUE' }, status: 404 });
                }
                // Success 
                else {
                    res.status(200).send({ customer_id: result });
                }
            } // Unexpected Server Error
            else {
                next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
            }
        });
    }
    // Validation failed - queue_id
    else if (!queueIdValidator) {
        next({ body: errors.INVALID_BODY_QUEUE.body, status: errors.INVALID_BODY_QUEUE.status });
    }// Unexpected error
    else {
        next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
    }
})

/**
 * Company: Arrival Rate
 */

app.get('/company/arrival_rate', function (req, res, next) {
    var queueIdCaseSensitive = req.query.queue_id;
    var from = req.query.from;
    var duration = parseInt(req.query.duration);
    // JSON validation
    var queueIdValidator = validator.isValid(queueIdCaseSensitive, validator.checkQueueId);
    var timeValidator = moment(from).isValid();
    var durationvalidator = validator.isValid(duration, validator.checkduration);
    // JSON validation passed
    if (queueIdValidator && timeValidator && durationvalidator) {
        from = moment(from).format('YYYY-M-D HH:mm:ss')
        duration = duration * 60;
        var endtime = moment(from).add(duration, 'seconds');
        endtime = moment(endtime).format('YYYY-M-D HH:mm:ss');
        // Convert to uppercase
        var queue_id = queueIdCaseSensitive.toUpperCase();
        // Connect to database
        database.arrivalRate(queue_id, from, endtime, function (err, result) {
            if (!err) {
                // Queue Id not found
                if (result == 'NOEXIST') {
                    next({ body: { error: "Queue Id '" + queue_id + "' Not Found", code: 'UNKNOWN_QUEUE' }, status: 404 });
                }
                // Arrival rate does not exist
                else if (result == 'TIMEERROR') {
                    next({ body: { error: "The arrival rate does not exist ", code: 'UNKOWN_TIME' }, status: 404 });
                }
                // Success
                else {
                    const output = [];
                    for (let i = 0; i < duration; i++) {
                        for (let a = 0; a < result.length; a++) {
                            if (moment(from).add(i, 'seconds').add(8, 'hours').format('YYYY-M-DTHH:mm:ss.000[Z]') == moment(result[a].timestamp).subtract(8, 'hours').format('YYYY-M-DTHH:mm:ss.000[Z]')) {
                                output[i] = result[a];
                            }
                            else {
                                output[i] = { timestamp: moment(from).add(i, 'seconds').format('YYYY-M-DTHH:mm:ss.000[Z]'), count: "0" };
                            }
                        }
                    }
                    Object.assign(result, output);
                    res.status(200).send(output);
                    //res.status(200).send(result);
                }
            }
            // Unexpected error
            else {
                next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
            }
        });
    }// Validation failed - queue_id
    else if (!queueIdValidator) {
        next({ body: errors.INVALID_QUERY_QUEUE.body, status: errors.INVALID_QUERY_QUEUE.status });
    }// Validation failed - time validator 
    else if (!timeValidator) {
        next({ body: { error: "Date and Time format is incorrect", code: 'INVALID_QUERY_STRING' }, status: 400 });
    }// Duration validator
    else if (!durationvalidator) {
        next({ body: { error: "INVALID Duration", code: 'INVALID_QUERY_STRING' }, status: 400 });
    }// Unexpected error
    else {
        next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
    }
});

app.get('/company/queue', function (req,res,next) {
    var company_id = req.query.company_id;
    var check10digits = validator.isValid(company_id, validator.check10digit);
    // If pass the JSON validation
        database.getQueue(company_id, function (err, result) {
            if (!err) {
                const output = [{i: 1}];
                if(result == "") {
                    res.status(200).send(output);
                }
                        for (let a = 0; a < result.length; a++) {
                            const status = 0
                            if (result[a].status == true) {
                                status = 1
                            }
                            output[a] = { queue_id: result[a].queue_id,is_active: status}
                        }
                    res.status(200).send(output);
            }
            else {
                next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
            }
        });
})

/**
 * ========================== CUSTOMER =========================
 */

/**
 * Customer: Join Queue
 */
app.post('/customer/queue', function (req, res, next) {
    var customer_id = req.body.customer_id;
    var queueIdCaseSensitive = req.body.queue_id;
    // JSON validation
    var queueIdValidator = validator.isValid(queueIdCaseSensitive, validator.checkQueueId);
    var customeridvalidator = validator.isValid(customer_id, validator.check10digit);
    // Validation passed
    if (queueIdValidator && customeridvalidator) {
        var queue_id = queueIdCaseSensitive.toUpperCase();
        database.joinQueue(customer_id, queue_id, function (err, result) {
            if (!err) {
                // Queue is active
                if (result !== false) {
                    // Customer already in queue
                    if (result == 'EXIST') {
                        next({ body: { error: "Customer '" + customer_id + "' is already in Queue '" + queue_id + "'", code: 'ALREADY_IN_QUEUE' }, status: 404 });
                    }// Success
                    else if (result == 'SUCCESS') {
                        res.status(201).send();
                    }// Queue_id not found
                    else {
                        next({ body: { error: "Queue Id '" + queue_id + "' Not Found", code: 'UNKNOWN_QUEUE' }, status: 404 });
                    }
                }
                // Queue is inactive
                else if (result == false) {
                    next({ body: { error: "Queue Id '" + queue_id + "' is INACTIVE", code: 'INACTIVE_QUEUE' }, status: 422 });
                }
                // Unexpected server error
                else {
                    next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 555 });
                }
            }
        });
    }
    // Invalid customer id
    else if (!customeridvalidator) {
        next({ body: errors.INVALID_BODY_CUSTOMER.body, status: errors.INVALID_BODY_CUSTOMER.status });
    }
    // Invalid queue id
    else if (!queueIdValidator) {
        next({ body: errors.INVALID_BODY_QUEUE.body, status: errors.INVALID_BODY_QUEUE.status });
    }
    // Unexpected server error
    else {
        next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
    }
});
/**
 * Customer: Check Queue
 */
app.get('/customer/queue', function (req, res, next) {
    var customer_id = req.query.customer_id;
    if (customer_id !== undefined) customer_id = parseInt(req.query.customer_id)
    var queueIdCaseSensitive = req.query.queue_id;
    // JSON validation
    var queueIdValidator = validator.isValid(queueIdCaseSensitive, validator.checkQueueId);
    var customeridvalidator = validator.isValid(customer_id, validator.check10digit);
    // If customer_id is not provided let validator pass through
    if (customer_id == undefined) customeridvalidator = true;
    // Validation passed
    if (queueIdValidator && customeridvalidator) {
        var queue_id = queueIdCaseSensitive.toUpperCase();
        database.checkQueue(queue_id, customer_id, function (err, result) {
            var output = { total: 0, ahead: -1, status: 'INACTIVE' }
            if (!err) {
                // Queue active
                console.log(result)
                if (result.status == true) output.status = 'ACTIVE';
                // Queue id not found/exist
                if (result == 'NOEXIST') {
                    next({ body: { error: "Queue Id '" + queue_id + "' Not Found", code: 'UNKNOWN_QUEUE' }, status: 404 });
                }
                else {
                    // Total queue number
                    output.total = result.total - result.current_queue_number;
                    result.current_queue_number = +1;
                    // Missed queue
                    if (result.queue_number < result.current_queue_number) {
                        res.status(200).send(output);
                    }
                    // Position in queue
                    else if (result.queue_number > result.current_queue_number) {
                        output.ahead = result.queue_number - result.current_queue_number;
                        res.status(200).send(output);
                    }
                    // Next to be assigned
                    else if (result.queue_number == result.current_queue_number) {
                        output.ahead = 0;
                        res.status(200).send(output);
                    }
                    // Never joined
                    else if (!result.queue_number) {
                        res.status(200).send(output);
                    }
                    // Queue exist but customer not inside
                    else {
                        res.status(200).send(output);
                    }
                }
            }
            else {
                next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
            }
        });
    }
    // Invalid customer id
    else if (!customeridvalidator) {
        next({ body: errors.INVALID_QUERY_CUSTOMER.body, status: errors.INVALID_QUERY_CUSTOMER.status });
    }
    // Invalid queue id
    else if (!queueIdValidator) {
        next({ body: errors.INVALID_QUERY_QUEUE.body, status: errors.INVALID_QUERY_QUEUE.status });
    }
    else {
        next({ body: { error: err.message, code: 'UNEXPECTED_ERROR' }, status: 500 });
    }
});

/**
 * ========================== UTILS =========================
 */
/**
 * 404
 */

const errors = {
    INVALID_BODY_COMPANY: {
        body: { error: 'Company Id should be 10-digits', code: 'INVALID_JSON_BODY' },
        status: 400,
    },
    INVALID_BODY_CUSTOMER: {
        body: { error: 'Customer Id should be 10-digits', code: 'INVALID_JSON_BODY' },
        status: 400,
    },
    INVALID_QUERY_CUSTOMER: {
        body: { error: 'Customer Id should be 10-digits', code: 'INVALID_QUERY_STRING' },
        status: 400,
    },
    INVALID_BODY_QUEUE: {
        body: { error: 'Queue Id should be 10-character alphanumeric string', code: 'INVALID_JSON_BODY' },
        status: 400,
    },
    INVALID_QUERY_QUEUE: {
        body: { error: 'Queue Id should be 10-character alphanumeric string', code: 'INVALID_QUERY_STRING' },
        status: 400,
    },
    INVALID_BODY_STATUS: {
        body: { error: 'Status must be either ACTIVATE or DEACTIVATE', code: 'INVALID_JSON_BODY' },
        status: 400,
    }
};

/**
 * Error Handler
 */
app.use(function (err, req, res, next) {
    const status = err.status || 500;
    const body = err.body || {
        error: 'Unknown Error!',
        code: 'UNKNOWN_ERROR'
    };
    res.status(status).send(body);
});

function tearDown() {
    // DO NOT DELETE
    return database.closeDatabaseConnections();
}

/**
 *  NOTE! DO NOT RUN THE APP IN THIS FILE.
 *
 *  Create a new file (e.g. server.js) which imports app from this file and run it in server.js
 */

module.exports = { app, tearDown }; // DO NOT DELETE