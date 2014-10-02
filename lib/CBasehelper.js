var async       = require('async')
var HTTPStatus  = require('http-status');
var couchbase   = require('couchbase')
var uuid        = require('uuid');

var dbc         = require('dbc');
var openiLogger = require('openi-logger')
var zmq         = require('m2nodehandler')
var openiUtils  = require('openi-cloudlet-utils')
var createViews = require('./couchbaseViews')
var conf        = require('./config.js');

var logger      = null;
var dbs         = {};


var init = function (logger_params) {
   logger = openiLogger(logger_params)

   dbs['openi'] = new couchbase.Connection({host: 'localhost:8091', bucket: 'openi'}, function(err){
      if (err) {
         console.log('Connection Error', err);
      } else {
         console.log('Connected openi bucket!');
      }
   });

   dbs['attachments'] = new couchbase.Connection({host: 'localhost:8091', bucket: 'attachments'}, function(err){
      if (err) {
         console.log('Connection Error', err);
      } else {
         console.log('Connected attachments bucket!');
      }
   });

   dbs['permissions'] = new couchbase.Connection({host: 'localhost:8091', bucket: 'permissions'}, function(err){
      if (err) {
         console.log('Connection Error', err);
      } else {
         console.log('Connected permissions bucket!');
      }
   });

   createViews(dbs['openi'], logger)

}


var getCouchbaseError = function(err, documentName, id){

   switch (err.code){
   case 4:
      return 'Object too big';
      break;
   case 13:
      return 'Entity with that id does not exist';
      break;
   case 12:
      if (0 === documentName.indexOf('t_')){
         return 'OPENi Type already exists (' + id + ').';
      }
      return 'Incorrect revision number provided';
      break;
   default:
      return "Reason unknown"
      break;
   }
}

var getDb = function(name){
   switch(name){
   case "attachments":
      return dbs['attachments']
   case "permissions":
      return dbs['permissions']
   default:
      return dbs['openi']
   }
}


var postAction = function (msg, callback) {

   var options = (undefined     === msg.format) ? {} : {format: msg.format }
   var db_use  = getDb(msg['bucket'])

   db_use.add(msg.database, msg.object_data, options, function (err, result) {
      if (err) {
         var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;
         callback(null,
            { 'error' : 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) },
            httpCode)
      }
      else {
         callback(null, { 'id' : msg.id }, HTTPStatus.CREATED)
      }
   })
}


var putAction = function (msg, callback) {

   var db_use  = getDb(msg['bucket'])

   db_use.get(msg.database, function (err, db_body) {

      var revisionParts = msg.revision.split('-');

      if (db_body.cas['0'] != revisionParts[0] && db_body.cas['1'] != revisionParts[1]){
         callback(null, { 'error' : 'Entity already updated'},
            HTTPStatus.CONFLICT)
         return;
      }

      if (db_body.value.openi_type === msg.object_data.openi_type
            && db_body.cas['1'] != revisionParts[1]){
         callback(null, { 'error' : 'Entity already updated'},
            HTTPStatus.CONFLICT)
         return;
      }


      var options = (undefined === msg.format) ? {} : {format: msg.format }


      db_use.set(msg.database, msg.object_data, options, function (err,result) {

         if (err) {
            logger.log('error', err )

            var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;

            callback(null, { 'error' : 'Error updating entity: ' + getCouchbaseError(err) },
               httpCode)
         }
         else {
            callback(null, { 'id' : msg.id }, HTTPStatus.OK)
         }
      })
   })
}


var processGetData = function(db_body, msg){

   var headers = zmq.header_json

   var resp = {}

   switch(msg['resp_type']){
   case 'cloudlet':
      resp = openiUtils.cloudletHelper(db_body, msg)
      break;
   case 'binary':
      headers  = {'Content-Type': db_body.value["Content-Type"]}
      resp = new Buffer(db_body.value['file'], 'base64')
      break;
   case 'binary_meta':
      db_body.value['file'] = undefined
      resp = db_body.value
      break;
   case 'type':
      resp = openiUtils.typeHelper(db_body, msg)
      break;
   case 'object':
   default:
      resp = openiUtils.objectHelper(db_body, msg)
      break;
   }

   return {'headers':headers, 'resp':resp}
}


var getOrPostAction = function (msg, callback) {

   var db_use  = getDb(msg['bucket'])

   db_use.get(msg.database, function (err, db_body) {

      if (err) {
         postAction(msg, function(a, b, c){
            if (c === HTTPStatus.CREATED && undefined !== msg.token){
               var key = msg.token.user + "-" + msg.object_data.id
               var permissions = {}
               //read, write, admin permissions
               permissions[msg.object_data.id] = 'rwa'

               getDb("permissions").add(key, permissions, function (err, result) {
                  if (err) {
                     var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;
                     callback(null,
                        { 'error' : 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) },
                        httpCode)
                  }
                  else {
                     callback(a, b, c)
                  }
               })
            }
            else{
               callback(a, b, c)
            }
         })
      }
      else {
         var response = processGetData(db_body, msg)

         callback(null, response.resp, HTTPStatus.OK, response.headers )
      }
   })

}


var getAction = function (msg, callback) {

   var db_use  = getDb(msg['bucket'])

   db_use.get(msg.database, function (err, db_body) {

      if (err) {
         logger.log('error', err )
         callback(null, { 'error' : 'Error getting data from datastore' }, HTTPStatus.INTERNAL_SERVER_ERROR)
      }
      else {
         var response = processGetData(db_body, msg)

         callback(null, response.resp, HTTPStatus.OK, response.headers )
      }
   })

}


var viewAction = function (msg, callback) {

   msg.count    = (typeof msg.count !== 'number' || isNaN(msg.count) || msg.count > 30) ? 30 : msg.count;
   msg.skip     = (typeof msg.skip  !== 'number' || isNaN(msg.skip))                    ? 0  : msg.skip;
   msg.startkey = typeof msg.startkey !== 'string' ? "" : msg.startkey;

   var params = {
      startkey_docid : msg.startkey,
      skip           : msg.skip,
      stale          : false,
      limit          : msg.count
   }

   if (undefined !== msg.key){
      params.startkey = msg.key;
      params.endkey   = msg.key;
   }

   if ( undefined !== msg.group && msg.group ){
      params.group = msg.group
   }

   getDb('openi').view(msg.design_doc, msg.view_name, params).query(function(err, res)
   {
      var respObj = [];

      for (var i = 0; i < res.length; i++){

         if (msg.resp_type === 'type'){
            respObj[i] = openiUtils.typeHelper(res[i], msg)
         }
         else if (msg.resp_type === 'type_stats') {
            respObj[i] = openiUtils.typeStats(res[i], msg)
         }
         else if(msg.resp_type === 'cloudlet'){
            respObj[i] = openiUtils.cloudletHelper(res[i], msg)
         }
         else{
            respObj[i] = openiUtils.objectHelper(res[i], msg)
         }
      }

      callback(null,  respObj, HTTPStatus.OK)
   });


}

var fetchAction = function (msg, callback) {

   getDb('openi').get(msg.database, function (err, db_body) {

      if (err) {
         logger.log('error', err )
         callback(null,
            { 'error' : 'Error getting entity: ' + getCouchbaseError(err) },
               HTTPStatus.INTERNAL_SERVER_ERROR )
      }
      else {
         callback(null,  { 'data' : db_body }, HTTPStatus.OK)
      }
   })

}


var createDBAction = function(msg, callback ){

   getDb('openi').add(msg.database, {}, function(err, res)
	{

		if(err && err.code != 12)
		{
         //logger.log('error', err)
    	    callback(null,
             { 'error' : 'Error creating entity: ' + getCouchbaseError(err) },
             HTTPStatus.INTERNAL_SERVER_ERROR )
      }
      else
      {
        	logger.log('debug', 'Cloudlet created, id: ' + msg.database )
        	callback(null, { 'id' : msg.database }, HTTPStatus.OK)
      }
	});
}


var deleteDBAction = function( msg, callback ){

   getDb(msg['bucket']).remove(msg.database, function (err) {

      if (err){
         logger.log('error', err)
         callback(null, { 'error' : 'Error deleting entity : ' + getCouchbaseError(err) }, HTTPStatus.NOT_FOUND)
      }
      else{
         logger.log('debug', 'entity deleted, id: ' + msg.database )
         callback(null, { 'id' : msg.id }, HTTPStatus.OK)
      }
   })
}


var evaluateAction = function (msg, callback) {

   dbc.conditionalHasMember(msg, 'database', (msg.action !== 'VIEW') )
   dbc.hasMemberIn(         msg, 'action',      ['POST', 'PUT', 'GET', 'FETCH', 'CREATE', 'DELETE', 'VIEW', 'GET_OR_POST'])
   dbc.conditionalHasMember(msg, 'object_data', (msg.action === 'POST'   || msg.action === 'PUT'))
   dbc.conditionalHasMember(msg, 'revision',    (msg.action === 'PUT'))

   var resp_msg = ''


   switch (msg.action){
   case 'POST':
      resp_msg = postAction     (msg, callback)
      break;
   case 'PUT':
      resp_msg = putAction     (msg, callback)
      break;
   case 'GET':
      resp_msg = getAction     (msg, callback)
      break;
   case 'GET_OR_POST':
      resp_msg = getOrPostAction(msg, callback)
      break;
   case 'FETCH':
      resp_msg = fetchAction   (msg, callback)
      break;
   case 'CREATE':
      resp_msg = createDBAction(msg, callback)
      break;
   case 'DELETE':
      resp_msg = deleteDBAction(msg, callback)
      break;
   case 'VIEW':
      resp_msg = viewAction     (msg, callback)
      break;
   }
   return resp_msg;
}

var actionToFunction = function(action) {
   return ( function(action) {
      return function(callback){
         evaluateAction(action, callback)
      }
   }(action) )
}


var evaluateMessage = function (msg, myCallback) {

   dbc.hasMember(msg, 'dao_actions'  )
   dbc.hasMember(msg, 'clients'      )

   dbc.assert(msg.clients.length     > 0)
   dbc.assert(msg.dao_actions.length > 0)

   var arr = {}

   for ( var i = 0; i < msg.dao_actions.length; i++){

      var action = msg.dao_actions[i]
      arr[i]     = actionToFunction(action)

   }

   async.series(arr, function(err, results, httpStatusCode){
      myCallback(err, results, httpStatusCode)
   })

}

module.exports.init            = init
module.exports.evaluateMessage = evaluateMessage
