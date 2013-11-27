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

	var mongPush = zmq.bindToMong2PubQ({
		spec : params.mongrel_sub_q.spec,
		id   : params.mongrel_sub_q.id
	})


	zmq.bindToPullQ({
		spec : params.dao_sub_q.spec,
		id   : params.dao_sub_q.id
	}, function(msg) {
		helper.evaluateMethod(msg, function(response) {
			if (null != response) {
				mongPush.publish(msg.uuid, msg.connId, response)
			}
		})		

	})
}

module.exports = dao