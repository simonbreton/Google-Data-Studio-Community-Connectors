// Connector workflow
// #1 User select connector in Data Studio
// #2 getAuthType() => Authflow
// #2 Requiered: getAuthType(), resetAuth(), isAuthValid(), setCredentials()
// #3 getConfig() => Specific requierements
// #4 getSchema() => Data sources edit screen 
// #5 getData() => Show data in chart
// https://codelabs.developers.google.com/codelabs/community-connectors
// https://developers.google.com/datastudio/connector

/**
 * Returns the Auth Type of this connector.
 * @return {object} The Auth type.
 */
// https://developers.google.com/datastudio/connector/auth#getauthtype
function getAuthType() {
    var cc = DataStudioApp.createCommunityConnector();
    return cc.newAuthTypeResponse()
        .setAuthType(cc.AuthType.USER_TOKEN)
        .setHelpUrl('https://www.example.org/connector-auth-help')
        .build();
}

/**
 * Resets the auth service.
 */
// https://developers.google.com/datastudio/connector/auth#resetauth
function resetAuth() {
    var user_tokenProperties = PropertiesService.getUserProperties();
    user_tokenProperties.deleteProperty('dscc.username');
    user_tokenProperties.deleteProperty('dscc.token');
}

/**
 * Returns true if the auth service has access.
 * @return {boolean} True if the auth service has access.
 */
// https://developers.google.com/datastudio/connector/auth#isauthvalid
function isAuthValid() {
    var userProperties = PropertiesService.getUserProperties();
    var username = userProperties.getProperty('dscc.username');
    var token = userProperties.getProperty('dscc.token');
    // This assumes you have a validateCredentials function that
    // can validate if the userName and token are correct.
    return validateCredentials(username, token);
}

/**
 * Sets the credentials.
 * @param {Request} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
// https://developers.google.com/datastudio/connector/auth#setcredentials
function setCredentials(request) {
    var creds = request.userToken;
    var username = creds.username;
    var token = creds.token;

    // Optional
    // Check if the provided username and token are valid through a
    // call to your service. You would have to have a `checkForValidCreds`
    // function defined for this to work.
    var validCreds = validateCredentials(username, token);

    if (!validCreds) {
        return {
            errorCode: 'INVALID_CREDENTIALS'
        };
    }

    var userProperties = PropertiesService.getUserProperties();
    userProperties.setProperty('dscc.username', username);
    userProperties.setProperty('dscc.token', token);
    return {
        errorCode: 'NONE'
    };
}

// credentials validation logic here.
function validateCredentials(username, token) {
    var rawResponse = UrlFetchApp.fetch(
        'url', {
            method: 'GET',
            headers: {
                'Authorization': username + ':' + token
            },
            muteHttpExceptions: true
        }
    );

    var responseCode = rawResponse.getResponseCode()

    if (responseCode === 200) {
        return true
    } else {
        return false
    }
}

// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig(request) {
    var cc = DataStudioApp.createCommunityConnector();
    var config = cc.getConfig();
    return config.build();
}


// https://developers.google.com/datastudio/connector/reference#getschema
function getFields(request) {
    var cc = DataStudioApp.createCommunityConnector();
    var fields = cc.getFields();
    var types = cc.FieldType;
    var aggregations = cc.AggregationType;

    fields.newMetric()
        .setId('metrics1')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);

    fields.newMetric()
        .setId('metrics2')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);

    fields.newDimension()
        .setId('date')
        .setType(types.YEAR_MONTH_DAY);

    fields.newDimension()
        .setId('dimension1')
        .setType(types.TEXT);

    fields.newDimension()
        .setId('dimension2')
        .setType(types.TEXT);

    return fields;
}

function getSchema(request) {
    var fields = getFields(request).build();
    return {
        schema: fields
    };
}


function responseToRows(requestedFields, response) {
    // Transform parsed data and filter for requested fields
    return response.map(function(transactions) {
        var row = [];
        requestedFields.asArray().forEach(function(field) {
            switch (field.getId()) {
                case 'metrics1':
                    return row.push(obj.metrics1);
                case 'metrics1':
                    return row.push(obj.metrics2);
                case 'date':
                    return row.push(obj.date);
                case 'dimension1':
                    return row.push(obj.dimension1);
                case 'dimension2':
                    return row.push(obj.dimension2);
                default:
                    return row.push('');
            }
        });
        console.log(row)
        return {
            values: row
        };

    });
}

// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
    var requestedFieldIds = request.fields.map(function(field) {
        return field.name;
    });
    var requestedFields = getFields().forIds(requestedFieldIds);

    var userProperties = PropertiesService.getUserProperties();
    var username = userProperties.getProperty('dscc.username');
    var token = userProperties.getProperty('dscc.token');

    var rawResponse = UrlFetchApp.fetch(
        'url', {
            method: 'GET',
            headers: {
                'Authorization': username + ':' + token
            },
            muteHttpExceptions: true
        }
    );

    var parsedResponse = JSON.parse(rawResponse)
    var rows = responseToRows(requestedFields, parsedResponse);

    return {
        schema: requestedFields.build(),
        rows: rows
    };
}
