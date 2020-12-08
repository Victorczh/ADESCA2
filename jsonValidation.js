// check 10 digits integer
const check10digit = {
    type: 'integer',
    minimum: 1000000000,
    maximum: 9999999999,
}
// check 10-character alphanumeric string
const checkQueueId = {
    type: 'string',
    pattern: '^[a-zA-Z0-9_]*$',
    minLength: 10,
    maxLength: 10
}
// check status
const checkStatus = {
    type: 'string',
    pattern: '^(ACTIVATE|DEACTIVATE)$'
}
// check duration
const checkduration = {
    type: 'integer',
    minimum: 1,
    maximum: 1440
}

const jsonschema = require('jsonschema');

function isValid(instance, schema) {
    if ((jsonschema.validate(instance, schema).errors.length) == 0) {
        return true;
    } else {
        return false;
    }
}

module.exports = {
    check10digit,
    checkQueueId,
    checkStatus,
    checkduration,
    isValid,
};