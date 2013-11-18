/*
 * DAO
 *
 *
 * Copyright (c) 2013 dconway, dmccarthy
 * Licensed under the MIT license.
 */

'use strict';

var zmq    = require('m2nodehandler')
var helper = require('./helper.js')

var dao = function(params) {

   helper.init(params.logger_params, function() {
		console.log('Database Destroyed and Created')
	})


//	var dataPush = zmq.bindToPushQ({
//		spec: params.data_api_in_q.spec
//	})


	var mongPush = zmq.bindToMong2PubQ({
		spec: params.mong_in_q.spec,
		id: params.mong_in_q.id
	})


	zmq.bindToPullQ({
		spec: params.data_api_out_q.spec,
		id: params.data_api_out_q.id
	}, function(msg) {
		helper.evaluateMethod(msg, function(response) {
			if (null != response) {
				mongPush.publish(msg.uuid, msg.connId, response)
			}
		})		

	})
}

module.exports = dao