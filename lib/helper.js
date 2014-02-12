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
var zmq         = require('m2nodehandler')
var logger      = null;

var init = function (logger_params) {
   logger = openiLogger(logger_params)
}


var putAction = function (msg, callback) {

   var db = nano.use(msg.cloudlet);

   db.insert(msg.object_data, msg.object_name, function (err) {

      var status = zmq.status.OK_200

      if (err) {
         logger.log('error', err )
         status                  = zmq.status.INTERNAL_SERVER_ERROR_500
         msg.mongrel_resp = { 'value' : false, error : 'Error adding data to datastore' }
      }
      else {
         msg.mongrel_resp.value  = true
      }
      var response = zmq.Response(status, zmq.header_json, msg.mongrel_resp)

      callback(response)
   })
}


var getAction = function (msg, callback) {

   var db = nano.use(msg.cloudlet)

   db.get(msg.object_name, {revs_info: false}, function (err, db_body) {

      var status = zmq.status.OK_200

      if (err) {
         logger.log('error', err )
         status                  = zmq.status.INTERNAL_SERVER_ERROR_500
         msg.mongrel_resp = { 'value' : false, error : 'Error getting data from datastore' }
      }
      else {
         msg.mongrel_resp.value  = true
         msg.mongrel_resp.data   = db_body
      }

      var response = zmq.Response(status, zmq.header_json, msg.mongrel_resp)
      callback(response)
   })

}


var fetchAction = function (msg, callback) {

   var db = nano.use(msg.cloudlet)

   db.fetch(msg.object_name, {revs_info: false}, function (err, db_body) {

      var status = zmq.status.OK_200

      if (err) {
         logger.log('error', err )
         status                  = zmq.status.INTERNAL_SERVER_ERROR_500
         msg.mongrel_resp = { 'value' : false, error : 'Error exporting data from datastore' }
      }
      else {
         msg.mongrel_resp.data = db_body
      }

      var response = zmq.Response(status, zmq.header_json, msg.mongrel_resp)

      callback(response)
   })

}


var createDBAction = function( msg, callback ){

   nano.db.create(msg.cloudlet, function (err) {

      if (err){
         logger.log('error', err)
         msg.mongrel_resp = { 'value' : false, error : 'Error creating Cloudlet' }
         var response     = zmq.Response(zmq.status.OK_200, zmq.header_json, msg.mongrel_resp)
         callback(response)
      }
      else{
         logger.log('debug', 'cloudlet created, id: ' + msg.object_name )
         msg.action = 'PUT'
         putAction(msg, callback)
      }
   })
}


var deleteDBAction = function( msg, callback ){

   nano.db.destroy(msg.cloudlet, function (err) {

      var response

      if (err){
         logger.log('error', err)
         msg.mongrel_resp = { 'value' : false, error : 'Error deleting Cloudlet' }
         response         = zmq.Response(zmq.status.OK_200, zmq.header_json, msg.mongrel_resp)
      }
      else{
         logger.log('debug', 'cloudlet deleted, id: ' + msg.object_name )
         response = zmq.Response(zmq.status.OK_200, zmq.header_json, msg.mongrel_resp)
      }
      callback(response)
   })
}


var evaluateMethod = function (msg, callback) {

   dbc.hasMemberIn(         msg, 'action',      ['POST', 'PUT', 'GET', 'FETCH', 'CREATE', 'DELETE'])
   dbc.conditionalHasMember(msg, 'object_name', (msg.action !== 'DELETE'))
   dbc.conditionalHasMember(msg, 'object_data', (msg.action === 'POST' || msg.action === 'PUT' || msg.action === 'CREATE'))
   dbc.hasMember(           msg, 'mongrel_resp' )

   switch (msg.action){
   case 'POST':
      putAction(msg, function (returned) {
         callback(returned)
      })
      break;
   case 'PUT':
      //slightly different from POST as updates require a revision number
      msg.object_data._rev = msg.revision
      putAction(msg, function (returned) {
         callback(returned)
      })
      break;
   case 'GET':
      getAction(msg, function (returned) {
         callback(returned)
      })
      break;
   case 'FETCH':
      fetchAction(msg, function (returned) {
         callback(returned)
      })
      break;
   case 'CREATE':
      createDBAction(msg, function (returned) {
         callback(returned)
      })
      break;
   case 'DELETE':
      deleteDBAction(msg, function (returned) {
         callback(returned)
      })
      break;
   default:
      var body = {
         error: 'Action parameter is required'
      }
      callback(zmq.Response(zmq.status.BAD_REQUEST_400, zmq.header_json, body))
      break;
   }

}



module.exports.init           = init
module.exports.evaluateMethod = evaluateMethod