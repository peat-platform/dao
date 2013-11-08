/*
 * DAO
 * 
 *
 * Copyright (c) 2013 dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq  = require('m2nodehandler')
var nano = require('nano')('http://localhost:5984');
var dbc  = require('dbc');


var initDB = function(callback){
   nano.db.destroy('db', function() {
      nano.db.create('db', function() {
         callback()
      })
   })
}


var putAction = function(msg, mongPush){

   dbc.hasMember(msg, 'name')
   dbc.hasMember(msg, 'data')

   var db = nano.use('db');
   db.insert(msg.data, msg.name, function(err, db_body){
      var status = zmq.status.OK_200
      var body   = {}
      if(err){
         var status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = "failure"
         body['error']  = 'Error: ' + err['Error'];
      }
      else{
         body['result'] = "success"
      }
      var response  = zmq.Response(status, zmq.header_json, body)
      mongPush.publish(msg.uuid, msg.connId, response)
   })
}


var getAction = function(msg, mongPush){

   var db = nano.use('db');

   dbc.hasMember(msg, 'name')

   switch(msg.name.length)
   {
      case 1:
         var status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = "failure"
         body['error']  = "Error getting data from datastore";
         break

      case 2:
         db.get(msg.name[1], { revs_info: false }, function(err, db_body){
            var status = zmq.status.OK_200
            var body   = {}
            var doc = db_body[msg.name[0]]
            //console.log(doc)
            if(err){
               var status = zmq.status.INTERNAL_SERVER_ERROR_500
               body['result'] = "failure"
               body['error']  = "Error getting data from datastore";
            }
            else{
               body['result'] = "success"
               body['value']  = db_body
            }
            var response  = zmq.Response(status, zmq.header_json, doc)
            mongPush.publish(msg.uuid, msg.connId, response)
         })
         break

      case 3:
         db.get(msg.name[1], { revs_info: false }, function(err, db_body){
            var status = zmq.status.OK_200
            var body   = {}
            var doc = db_body[msg.name[0]]
            //console.log(doc)
            var object = doc[msg.name[2]]
            //console.log(object)
            if(err){
               var status = zmq.status.INTERNAL_SERVER_ERROR_500
               body['result'] = "failure"
               body['error']  = "Error getting data from datastore";
            }
            else{
               body['result'] = "success"
               body['value']  = db_body
            }
            var response  = zmq.Response(status, zmq.header_json, object)
            mongPush.publish(msg.uuid, msg.connId, response)
         })
         break


      case 4:
         db.get(msg.name[1], { revs_info: false }, function(err, db_body){
            var status = zmq.status.OK_200
            var body   = {}
            var doc = db_body[msg.name[0]]
            //console.log(doc)
            var object = doc[msg.name[2]]
            //console.log(object)

            var entry = object[msg.name[3]]

            if(err){
               var status = zmq.status.INTERNAL_SERVER_ERROR_500
               body['result'] = "failure"
               body['error']  = "Error getting data from datastore";
            }
            else{
               body['result'] = "success"
               body['value']  = db_body
            }
            var response  = zmq.Response(status, zmq.header_json, entry)
            mongPush.publish(msg.uuid, msg.connId, response)
         })
         break

      default:
         db.get(msg.name, { revs_info: false }, function(err, db_body){
               var status = zmq.status.OK_200
               var body   = {}
            if(err){
               var status = zmq.status.INTERNAL_SERVER_ERROR_500
               body['result'] = "failure"
               body['error']  = "Error getting data from datastore";
            }
            else{
               body['result'] = "success"
               body['value']  = db_body
            }
            var response  = zmq.Response(status, zmq.header_json, body)
            mongPush.publish(msg.uuid, msg.connId, response)
         })
   }

/*
   db.get(msg.name, { revs_info: false }, function(err, db_body){
      var status = zmq.status.OK_200
      var body   = {}
      if(err){
         var status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = "failure"
         body['error']  = "Error getting data from datastore";
      }
      else{
         body['result'] = "success"
         body['value']  = db_body
      }
      var response  = zmq.Response(status, zmq.header_json, body)


      mongPush.publish(msg.uuid, msg.connId, response)
   })
*/
}


var bindToQs = function(){

   var dataPush = zmq.bindToPushQ(    {spec:'tcp://127.0.0.1:49995'});
   var mongPush = zmq.bindToMong2PubQ({spec:'tcp://127.0.0.1:49996', id:'dao_conn'});

   var echo = 0;

   zmq.bindToPullQ( {spec:'tcp://127.0.0.1:49994', id:'data_api'}, function( msg ) {

      dbc.assert   (null !== msg, "Message cannot be null")
      dbc.hasMember(msg, 'action')
      dbc.hasMember(msg, 'uuid'  )
      dbc.hasMember(msg, 'connId')

      console.log("---GET ACTION----")

      console.log(msg)

      switch(msg.action){
         case 'PUT':
            putAction(msg, mongPush)
            break;
         case 'GET':
            getAction(msg, mongPush)
            break;
         case 'ECHO':
            msg.body = {echo: echo++}
            dataPush.push(msg)
            break;
         default:
            var body      = {error : 'Action parameter is required'};
            var response  = zmq.Response(zmq.status.BAD_REQUEST_400, zmq.header_json, body)
            mongPush.publish(msg.uuid, msg.connId, response)
            break;
      }
   });
}


initDB(function(){
   console.log("Database Destroyed and Created")
   bindToQs()
})