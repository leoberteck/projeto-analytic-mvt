function executeQuery(oracleCon, query, parameters = {}, options = {}) {
    if(Array.isArray(parameters)){
        return executeMany(oracleCon, query, parameters, options)
    } else {
        return new Promise((resolve, reject) => {
            oracleCon.execute(
                query
                , parameters
                , options
                , (err, result) => {
                    err != null ? reject(err) : resolve(result)
                }
            )
        })
    }
}

function executeMany(oracleCon, query, parameters = {}, options = {}) {
    return new Promise((resolve, reject) => {
        oracleCon.executeMany(
            query
            , parameters
            , options
            , (err, result) => {
                err != null ? reject(err) : resolve(result)
            }
        )
    })
}

function jsonStreamToObject(stream) {
    return new Promise((resolve, reject) => {
        var str = ''
        stream.on('data', (chunk) => {
            str += Buffer.from(chunk).toString('utf8')
        })
        stream.on('end', () => {
            resolve(JSON.parse(str))
        });
    })
}

module.exports = {
    executeQuery : executeQuery
    , jsonStreamToObject : jsonStreamToObject
}