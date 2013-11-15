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
var flag = false;

var initDB = function(callback){
   if (flag){
      return
   }
   nano.db.destroy('db', function() {
      nano.db.create('db', function() {
         flag = true;
         callback()
      })
   })
}

var handleTypeParameters = function(db,msg) {
	var query = msg.query
	var params = {}
	query.replace(/([^=&]+)=([^&]*)/gi, function(m, key, value) {
		params[key] = value
	})

	var object = null

	db.get(msg.name[1], {revs_info: false}, function(err, db_body) 
	{
		var doc
		var data
		var key
		if (typeof params['oids'] !== 'undefined' && params['oids'] === 'true') {
			doc = db_body['types']
			object = doc[msg.name[3]]
		} else if (typeof params['blob'] !== 'undefined' && params['blob'] === 'false') {
			doc = db_body['types']
			object = doc[msg.name[3]]
			data = db_body['data']
			for (key in object) {
				if (data.hasOwnProperty(object[key])) {
					object[key] = data[object[key]]
				}
			}
		} else if (typeof params['blob'] !== 'undefined' && params['blob'] === 'true') {
			doc = db_body['types']
			object = doc[msg.name[3]]
			var blobs = db_body['blobs']
			data = db_body['data']
			var blobkeys = Object.keys(blobs)
			for (key in object) {
				if (typeof key !== 'undefined') {
					console.log(blobkeys.indexOf(data[object[key]]))
					if (data.hasOwnProperty(object[key]) && (blobkeys.indexOf(data[object[key]]) !== -1)) {
						object[key] = blobs[data[object[key]]]
					}
				}
			}
		}

		return object
	})
}

var putAction = function(msg, mongPush){

   dbc.hasMember(msg, 'name')
   dbc.hasMember(msg, 'data')

   var db = nano.use('db');
   db.insert(msg.data, msg.name, function(err){
      var status = zmq.status.OK_200
      var body   = {}
      if(err){
         status = zmq.status.INTERNAL_SERVER_ERROR_500
         body['result'] = 'failure'
         body['error']  = 'Error: ' + err['Error'];
      }
      else{
         body['result'] = 'success'
      }
      var response  = zmq.Response(status, zmq.header_json, body)
      mongPush.publish(msg.uuid, msg.connId, response)
   })
}


var getAction = function(msg, mongPush){

	var db = nano.use('db')
	dbc.hasMember(msg, 'name')
	db.get(msg.name[1], {revs_info: false}, function(err, db_body) {
		var status = zmq.status.OK_200
		var body = {}
		var doc
		var object

		//console.log(doc)
		switch (msg.name.length) {
		case 2:
			object = db_body[msg.name[0]]
			break
		case 3:
			doc = db_body[msg.name[0]]
			object = doc[msg.name[2]]
			break
		case 4:
			if (msg.name[2] === 'type') {
				if (typeof msg.query !== 'undefined') {
					object = handleTypeParameters(db, msg)
				} 
				else 
				{
					doc = db_body['types']
					object = doc[msg.name[3]]
				}
			} else {
				doc = db_body['data']
				var field = doc[msg.name[2]]
				object = field[msg.name[3]]
			}
			break
		default:
			object = db_body
			break
		}


		if (err) {
			status = zmq.status.INTERNAL_SERVER_ERROR_500
			body['result'] = 'failure'
			body['error'] = 'Error getting data from datastore'
		} else {
			body['result'] = 'success'
			body['value'] = object
		}
		var response = zmq.Response(status, zmq.header_json, body)
		mongPush.publish(msg.uuid, msg.connId, response)
	})
}

var bindToQs = function(){

	var dataPush = zmq.bindToPushQ(     {spec:'tcp://127.0.0.1:49995'})
	var mongPush = zmq.bindToMong2PubQ({spec:'tcp://127.0.0.1:49996', id:'dao_conn'})

	var echo = 0

	zmq.bindToPullQ( {spec:'tcp://127.0.0.1:49994', id:'data_api'}, function( msg ) 
	{
		dbc.assert    (null !== msg, 'Message cannot be null')
		dbc.hasMember(msg, 'action')
		dbc.hasMember(msg, 'uuid'  )
		dbc.hasMember(msg, 'connId')
		console.log(msg)

		switch(msg.action){
		case 'PUT':
			putAction(msg, mongPush)
			break
            
		case 'GET':
			getAction(msg, mongPush)
			break
            
		case 'ECHO':
			msg.body = {echo: echo++}
			dataPush.push(msg)
			break
            
		default:
			var body        = {error : 'Action parameter is required'}
			var response  = zmq.Response(zmq.status.BAD_REQUEST_400, zmq.header_json, body)
			mongPush.publish(msg.uuid, msg.connId, response)
			break
		}
	})
}


initDB(function(){
	console.log('Database Destroyed and Created')
	bindToQs()
})

module.exports.getAction			= getAction
module.exports.initDB				= initDB
module.exports.handleTypeParameters	= handleTypeParameters

