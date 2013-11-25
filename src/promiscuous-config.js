'use strict';

//External
var rsvp = require('rsvp'),
    prom = require('promiscuous-tool');

prom.config({
    promise: function (resolver) {
        return new rsvp.Promise(function(fulfill, reject){
            resolver(fulfill, reject);
        });
    }
});
