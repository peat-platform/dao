/*
 * openi_data_api
 * openi-ict.eu
 *
 * Copyright (c) 2013 dconway
 * Licensed under the MIT license.
 */

'use strict';

var nano        = require('nano')('http://dev.openi-ict.eu:5984');
var dbc         = require('dbc');
var openiLogger = require('openi-logger')
var async       = require('async')
var logger      = null;

var init = function (logger_params) {
   logger = openiLogger(logger_params)
}


var putAction = function (msg, callback) {

   var db = nano.use(msg.database);

   db.insert(msg.object_data, msg.object_name, function (err) {

      if (err) {
         logger.log('error', err )
         callback(null, { 'value' : false, 'data' : 'Error adding data to datastore' })
      }
      else {
         callback(null, { 'value' : true, 'data' : 'Added Object: ' + msg.object_name })
      }
   })
}


var getAction = function (msg, callback) {

   var db = nano.use(msg.database)

   db.get(msg.object_name, {revs_info: false}, function (err, db_body) {

      if (err) {
         logger.log('error', err )
         callback(null, { 'value' : false, 'error' : 'Error getting data from datastore' })
      }
      else {
         callback(null,  { 'value' : true, 'data' : db_body })
      }
   })

}


var fetchAction = function (msg, callback) {

   console.log(msg)

   var db = nano.use(msg.database)

   db.fetch(msg.object_name, {revs_info: false}, function (err, db_body) {

      if (err) {
         logger.log('error', err )
         callback(null, { 'value' : false, 'error' : 'Error exporting data from datastore' })
      }
      else {
         callback(null, { 'value' : false, 'data' : db_body })
      }
   })

}


var createDBAction = function(msg, callback ){

   nano.db.create(msg.database, function (err) {
      if (err){
         logger.log('error', err)
         callback(null, { 'value' : false, 'error' : 'Error creating Cloudlet' } )
      }
      else{
         logger.log('debug', 'Cloudlet created, id: ' + msg.database )
         callback(null, { 'value' : true, 'data' : 'Database Created: ' + msg.database })
      }
   })
}


var deleteDBAction = function( msg, callback ){

   nano.db.destroy(msg.database, function (err) {

      if (err){
         logger.log('error', err)
         callback(null, { 'value' : false, error : 'Error deleting Database' })
      }
      else{
         logger.log('debug', 'cloudlet deleted, id: ' + msg.database )
         callback(null, { 'value' : true, 'data' : 'Database deleted: ' + msg.database })
      }
   })
}


var evaluateAction = function (msg, callback) {

   dbc.hasMember(           msg, 'database' )
   dbc.hasMemberIn(         msg, 'action',      ['POST', 'PUT', 'GET', 'FETCH', 'CREATE', 'DELETE'])
   dbc.conditionalHasMember(msg, 'object_name', (msg.action !== 'DELETE' && msg.action !== 'CREATE'))
   dbc.conditionalHasMember(msg, 'object_data', (msg.action === 'POST'   || msg.action === 'PUT'))
   dbc.conditionalHasMember(msg, 'revision',    (msg.action === 'PUT'))

   var resp_msg = ''

   switch (msg.action){
   case 'POST':
      resp_msg = putAction     (msg, callback)
      break;
   case 'PUT':
      //slightly different from POST as updates require a revision number
      msg.object_data._rev = msg.revision

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

   dbc.hasMember(msg, 'mongrel_resp' )
   dbc.hasMember(msg, 'dao_actions'  )
   dbc.hasMember(msg, 'clients'      )

   dbc.assert(msg.clients.length     > 0)
   dbc.assert(msg.dao_actions.length > 0)

   var arr = {}

   for ( var i = 0; i < msg.dao_actions.length; i++){

      var action = msg.dao_actions[i]
      arr[i]     = actionToFunction(action)

   }

   async.series(arr, function(err, results){
      myCallback(err, results)
   })

}


module.exports.init            = init
module.exports.evaluateMessage = evaluateMessage