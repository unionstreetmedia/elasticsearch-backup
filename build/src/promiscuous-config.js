var rsvp = require('rsvp');
require('promiscuous-tool').config({promise: function(resolver) {
    return new rsvp.Promise(function(fulfill, reject) {
      resolver(fulfill, reject);
    });
  }});
