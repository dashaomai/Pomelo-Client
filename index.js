/////////////////////////////////////////////////////////////
var Websocket = require('ws');
var Protocol = require('pomelo-protocol');
var Package = Protocol.Package;
var Message = Protocol.Message;
var Protobuf = require('pomelo-protobuf');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var logger = require('pomelo-logger').getLogger(__filename);

/**
 * Pomelo 的 nodejs 客户端，基于 Webthis.socket 连接。
 */
var PomeloClient = function() {
  this.socket = null;
  this.reqId = 0;
  this.callbacks = {};
  this.handlers = {};
  this.routeMap = {};

  this.heartbeatInterval = 5000;
  this.heartbeatTimeout = this.heartbeatInterval * 2;
  this.nextHeartbeatTimeout = 0;
  this.gapThreshold = 100; // heartbeat gap threshold
  this.heartbeatId = null;
  this.heartbeatTimeoutId = null;

  this.handshakeCallback = null;

  this.initCallback = null;

  this.handlers[Package.TYPE_HANDSHAKE] = this.handshake;
  this.handlers[Package.TYPE_HEARTBEAT] = this.heartbeat;
  this.handlers[Package.TYPE_DATA] = this.onData;
  this.handlers[Package.TYPE_KICK] = this.onKick;
};
util.inherits(PomeloClient, EventEmitter);

module.exports = PomeloClient;

PomeloClient.JS_WS_CLIENT_TYPE = 'js-websocket';
PomeloClient.JS_WS_CLIENT_VERSION = '0.0.1';
PomeloClient.handshakeBuffer = {
  'sys':{
    type: PomeloClient.JS_WS_CLIENT_TYPE,
    version: PomeloClient.JS_WS_CLIENT_VERSION
  },
  'user': {}
};

PomeloClient.RES_OK = 200;
PomeloClient.RES_OLD_CLIENT = 501;

var pomelo = PomeloClient.prototype;

pomelo.init = function(params, cb){
  this.params = params;
  // params.debug = true;
  this.initCallback = cb;
  var host = params.host;
  var port = params.port;

  var url = 'ws://' + host;
  if(port) {
    url +=  ':' + port;
  }

  if (!params.type) {
    logger.debug('init websocket');
    PomeloClient.handshakeBuffer.user = params.user;
    this.handshakeCallback = params.handshakeCallback;
    this.connect(url);
  }
};

pomelo.connect = function(url){
  this.disconnect();

  logger.debug('connect to: %s', url);

  var self = this;
  var onopen = function(event){
    logger.debug('[pomeloclient.init] websocket connected!');
    var obj = Package.encode(Package.TYPE_HANDSHAKE, Protocol.strencode(JSON.stringify(PomeloClient.handshakeBuffer)));
    self.send(obj);
  };
  var onmessage = function(event) {
    logger.debug('on message with data: %j', event.data);
    processPackage(self, Package.decode(event.data));
    // new package arrived, update the heartbeat timeout
    if(self.heartbeatTimeout) {
      self.nextHeartbeatTimeout = Date.now() + self.heartbeatTimeout;
    }
  };
  var onerror = function(event) {
    self.emit('io-error', event);
    logger.debug('socket error ' + event);
  };
  var onclose = function(event){
    self.emit('close',event);
    logger.debug('socket close ' + event);
  };

  this.socket = new Websocket(url);
  this.socket.binaryType = 'arraybuffer';
  this.socket.onopen = onopen;
  this.socket.onmessage = onmessage;
  this.socket.onerror = onerror;
  this.socket.onclose = onclose;
};

pomelo.disconnect = function() {
  if(this.socket) {
    if(this.socket.disconnect) this.socket.disconnect();
    if(this.socket.close) this.socket.close();

    if (this.socket.onopen) this.socket.onopen = null;
    if (this.socket.onmessage) this.socket.onmessage = null;
    if (this.socket.onerror) this.socket.onerror = null;
    if (this.socket.onclose) this.socket.onclose = null;

    logger.debug('disconnect');
    this.socket = null;
  }

  if(this.heartbeatId) {
    clearTimeout(this.heartbeatId);
    this.heartbeatId = null;
  }
  if(this.heartbeatTimeoutId) {
    clearTimeout(this.heartbeatTimeoutId);
    this.heartbeatTimeoutId = null;
  }
};

pomelo.request = function(route, msg, cb) {
  msg = msg || {};
  route = route || msg.route;
  if(!route) {
    logger.debug('fail to send request without route.');
    return;
  }

  this.reqId++;
  this.sendMessage(this.reqId, route, msg);

  this.callbacks[this.reqId] = cb;
  this.routeMap[this.reqId] = route;
};

pomelo.notify = function(route, msg) {
  msg = msg || {};
  this.sendMessage(0, route, msg);
};

pomelo.sendMessage = function(reqId, route, msg) {
  var type = reqId ? Message.TYPE_REQUEST : Message.TYPE_NOTIFY;

  //compress message by Protobuf
  var protos = !!this.data.protos ? this.data.protos.client : {};
  if(!!protos[route]){
    logger.debug('send message %j with protobuf format.', msg);
    msg = Protobuf.encode(route, msg);
  }else{
    logger.debug('send message %j with json format.', msg);
    msg = Protocol.strencode(JSON.stringify(msg));
    logger.debug('send message %j with json format.', msg);
  }

  var compressRoute = 0;
  if(this.dict && this.dict[route]){
    route = this.dict[route];
    compressRoute = 1;
  }

  msg = Message.encode(reqId, type, compressRoute, route, msg);
  var packet = Package.encode(Package.TYPE_DATA, msg);
  this.send(packet);
};

pomelo.send = function(packet){
  if (!!this.socket) {
    logger.debug('send packet: %j', packet);
    this.socket.send(packet.buffer || packet, {binary: true, mask: true});
  }
};

pomelo.heartbeat = function(data) {
  var obj = Package.encode(Package.TYPE_HEARTBEAT);
  if(this.heartbeatTimeoutId) {
    clearTimeout(this.heartbeatTimeoutId);
    this.heartbeatTimeoutId = null;
  }

  if(this.heartbeatId) {
    // already in a heartbeat interval
    return;
  }

  var self = this;
  this.heartbeatId = setTimeout(function() {
    self.heartbeatId = null;
    self.send(obj);

    self.nextHeartbeatTimeout = Date.now() + self.heartbeatTimeout;
    self.heartbeatTimeoutId = setTimeout(self.heartbeatTimeoutCb, self.heartbeatTimeout);
  }, this.heartbeatInterval);
};

pomelo.heartbeatTimeoutCb = function() {
  var gap = this.nextHeartbeatTimeout - Date.now();
  if(gap > this.gapThreshold) {
    this.heartbeatTimeoutId = setTimeout(this.heartbeatTimeoutCb, gap);
  } else {
    logger.error('server heartbeat timeout');
    this.emit('heartbeat timeout');
    this.disconnect();
  }
};

pomelo.handshake = function(data){
  data = JSON.parse(Protocol.strdecode(data));
  if(data.code === PomeloClient.RES_OLD_CLIENT) {
    this.emit('error', 'client version not fullfill');
    return;
  }

  if(data.code !== PomeloClient.RES_OK) {
    this.emit('error', 'handshake fail');
    return;
  }

  this.handshakeInit(data);

  var obj = Package.encode(Package.TYPE_HANDSHAKE_ACK);
  this.send(obj);
  if(this.initCallback) {
    this.initCallback(null, this.socket);
    this.initCallback = null;
  }
};

pomelo.onData = function(data){
  //probuff decode
  var msg = Message.decode(data);
  logger.debug('receive data: ' + msg);

  if(msg.id > 0){
    msg.route = this.routeMap[msg.id];
    delete this.routeMap[msg.id];
    if(!msg.route){
      return;
    }
  }

  msg.body = this.deCompose(msg);

  processMessage(this, msg);
};

pomelo.onKick = function(data) {
  this.emit('onKick');
};

var processPackage = function(client, msg){
  client.handlers[msg.type].call(client, msg.body);
};

var processMessage = function(client, msg) {
  if(!msg || !msg.id) {
    // server push message
    // logger.error('processMessage error!!!');
    client.emit(msg.route, msg.body);
    return;
  }

  //if have a id then find the callback function with the request
  var cb = client.callbacks[msg.id];

  delete client.callbacks[msg.id];
  if(typeof cb !== 'function') {
    return;
  }

  cb(null, msg.body);
  return;
};

var processMessageBatch = function(client, msgs) {
  for(var i=0, l=msgs.length; i<l; i++) {
    processMessage(client, msgs[i]);
  }
};

pomelo.deCompose = function(msg){
  var protos = !!this.data.protos ? this.data.protos.server : {};
  var abbrs = this.data.abbrs;
  var route = msg.route;

  try {
    //Decompose route from dict
    if(msg.compressRoute) {
      if(!abbrs[route]){
        logger.error('illegal msg!');
        return {};
      }

      route = msg.route = abbrs[route];
    }
    if(!!protos[route]){
      return Protobuf.decode(route, msg.body);
    }else{
      return JSON.parse(Protocol.strdecode(msg.body));
    }
  } catch(ex) {
    logger.error('route, body = ' + route + ", " + msg.body);
  }

  return msg;
};

pomelo.handshakeInit = function(data){
  if(data.sys && data.sys.heartbeat) {
    this.heartbeatInterval = data.sys.heartbeat * 1000;   // heartbeat interval
    this.heartbeatTimeout = this.heartbeatInterval * 2;        // max heartbeat timeout
  } else {
    this.heartbeatInterval = 0;
    this.heartbeatTimeout = 0;
  }

  this.initData(data);

  if(typeof this.handshakeCallback === 'function') {
    this.handshakeCallback(data.user);
  }
};

//Initilize data used in pomelo client
pomelo.initData = function(data) {
  if(!data || !data.sys) {
    return;
  }
  this.data = this.data || {};
  var dict = data.sys.dict;
  var protos = data.sys.protos;

  //Init compress dict
  if(!!dict){
    this.data.dict = dict;
    this.data.abbrs = {};

    for(var route in dict){
      this.data.abbrs[dict[route]] = route;
    }
  }

  //Init Protobuf protos
  if(!!protos){
    this.data.protos = {
      server : protos.server || {},
      client : protos.client || {}
    };
    if(!!Protobuf){
      Protobuf.init({encoderProtos: protos.client, decoderProtos: protos.server});
    }
  }
};
