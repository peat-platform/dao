var dbc         = require('dbc');
var openiLogger = require('openi-logger')
var async       = require('async')
var HTTPStatus  = require('http-status');

var couchbase   = require('couchbase')
var uuid        = require('uuid');

var conf        = require('./config.js');
var createViews = require('./couchbaseViews')

var logger      = null;
var db          = null;


var init = function (logger_params) {
   logger = openiLogger(logger_params)
   db = new couchbase.Connection({host: 'localhost:8091', bucket: 'openi'}, function(err){
      if (err) {
         console.log('Connection Error', err);
      } else {
         console.log('Connected!');
      }
   });

   createViews(db, logger)

}


var getCouchbaseError = function(err, documentName, rest_uuid){

   switch (err.code){
   case 13:
      return 'Entity with that id does not exist';
      break;
   case 12:
      if (0 === documentName.indexOf('t_')){
         return 'OPENi Type already exists (' + rest_uuid + ').';
      }
      return 'Incorrect revision number provided';
      break;
   default:
      return "Reason unknown"
      break;
   }
}


var postAction = function (msg, callback) {

   db.add(msg.database, msg.object_data, function (err, result) {

      if (err) {
         logger.log('error', err )
         var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;
         callback(null,
            { 'error' : 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.rest_uuid) },
            httpCode)
      }
      else {
         callback(null, { 'id' : msg.rest_uuid }, HTTPStatus.CREATED)
      }
   })
}


var putAction = function (msg, callback) {

   db.get(msg.database, function (err, db_body) {


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


      db.set(msg.database, msg.object_data, {cas: db_body.cas}, function (err,result) {

         if (err) {
            logger.log('error', err )

            var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;

            callback(null, { 'error' : 'Error updating entity: ' + getCouchbaseError(err) },
               httpCode)
         }
         else {
            callback(null, { 'id' : msg.rest_uuid }, HTTPStatus.OK)
         }
      })
   })
}


var objectHelper = function(db_object, filter_show){

   var type = db_object.value._openi_type;
   var id   = db_object.value.rest_uuid;

   db_object.value.openi_type = undefined;
   db_object.value.rest_uuid  = undefined;

   if (filter_show){
      for (var m in db_object.value._data){
         if (-1 === filter_show.indexOf(m) ){
            db_object.value._data[m] = undefined
         }
      }
   }

   var resp_obj          = db_object.value
   if (db_object.cas){
      resp_obj["_revision"] = db_object.cas["0"] + "-" + db_object.cas["1"]
   }

   return resp_obj
}


var getAction = function (msg, callback) {

   db.get(msg.database, function (err, db_body) {

      if (err) {
         logger.log('error', err )
         callback(null, { 'error' : 'Error getting data from datastore' }, HTTPStatus.INTERNAL_SERVER_ERROR)
      }
      else {

         logger.log('debug', db_body)

         var resp = {}

            if (0 === msg.database.indexOf('t_')){

            if (msg['content-type'] == "application/json-ld"){

               db_body.value['@type']    = db_body.value._reference
               db_body.value['@context'] = {}
               db_body.value['@id']      = db_body.value._id

               for ( var i in db_body.value._context ){
                  var entry = db_body.value._context[i]

                  var ldEntry = {'@id' : entry._property_context._id, '@type' : entry._property_context._openi_type }

                  db_body.value['@context'][entry._property_name] = ldEntry
               }

               db_body.value['_reference'] = undefined
               db_body.value['_context']   = undefined
               db_body.value['_id']        = undefined

            }

            var resp = db_body.value
         }
         else{
            var resp = objectHelper(db_body)
         }

         callback(null, resp, HTTPStatus.OK )
      }
   })

}


var viewAction = function (msg, callback) {

   console.log(msg)

   msg.count    = (typeof msg.count !== 'number' || isNaN(msg.count) || msg.count > 30) ? 30 : msg.count;
   msg.skip     = (typeof msg.skip  !== 'number' || isNaN(msg.skip))                    ? 0  : msg.skip;
   msg.startkey = typeof msg.startkey !== 'string' ? "" : msg.startkey;

   db.view(msg.design_doc, msg.view_name,
      {
         startkey_docid : msg.startkey,
         skip           : msg.skip,
         startkey       : msg.key,
         endkey         : msg.key,
         stale          : false,
         limit          : msg.count
      }).query(function(err, res)
   {

      var respObj = [];

      for (var i = 0; i < res.length; i++){
         respObj[i] = objectHelper(res[i], msg.filter_show)
      }

      callback(null,  respObj, HTTPStatus.OK)
   });


}

var fetchAction = function (msg, callback) {

   db.get(msg.database, function (err, db_body) {

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

	db.add(msg.database, {}, function(err, res)
	{

		if(err && err.code != 12)
		{
         logger.log('error', err)
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

   db.remove(msg.database, function (err) {

      if (err){
         logger.log('error', err)
         callback(null, { 'error' : 'Error deleting entity : ' + getCouchbaseError(err) }, HTTPStatus.NOT_FOUND)
      }
      else{
         logger.log('debug', 'entity deleted, id: ' + msg.database )
         callback(null, { 'id' : msg.database }, HTTPStatus.OK)
      }
   })
}


var evaluateAction = function (msg, callback) {

   dbc.conditionalHasMember(msg, 'database', (msg.action !== 'VIEW') )
   dbc.hasMemberIn(         msg, 'action',      ['POST', 'PUT', 'GET', 'FETCH', 'CREATE', 'DELETE', 'VIEW'])
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
