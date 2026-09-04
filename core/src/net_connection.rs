use crate::Player;
use crate::avm1::Object as Avm1Object;
use crate::avm1::globals::netconnection::NetConnection as Avm1NetConnectionObject;
use crate::avm2::amf::serialize_value;
use crate::avm2::object::{
    ArrayObject as Avm2ArrayObject, NetConnectionObject as Avm2NetConnectionObject,
    ResponderObject as Avm2ResponderObject, SharedObjectObject as Avm2SharedObjectObject,
};
use crate::avm2::{
    Activation as Avm2Activation, Avm2, EventObject as Avm2EventObject, FunctionArgs, Multiname,
    Value as Avm2Value,
};
use crate::backend::navigator::{
    ErrorResponse, FetchReason, NavigatorBackend, OwnedFuture, Request,
};
use crate::context::UpdateContext;
use crate::loader::Error;
use crate::rtmp::{RtmpConnection, RtmpEvent, RtmpSharedObjectEvent};
use crate::socket::{SocketHandle, Sockets};
use crate::string::AvmString;
use flash_lso::packet::{Header, Message, Packet};
use flash_lso::types::{AMFVersion, ObjectId, Value as AmfValue};
use gc_arena::{Collect, DynamicRoot, Gc, Rootable};
use slotmap::{SlotMap, new_key_type};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

new_key_type! {
    pub struct NetConnectionHandle;
}

#[derive(Debug)]
pub(crate) enum RtmpSocketEvent {
    Connected,
    Failed,
    Data(Vec<u8>),
    Closed,
}

#[derive(Debug)]
enum RtmpDispatch {
    Event(RtmpEvent),
    CallFailed(String),
    Responder {
        responder: ResponderHandle,
        callback: ResponderCallback,
        value: Rc<AmfValue>,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ResponderCallback {
    Result,
    Status,
}

#[derive(Clone)]
pub enum ResponderHandle {
    Avm2(DynamicRoot<Rootable![Avm2ResponderObject<'_>]>),
    Avm1(DynamicRoot<Rootable![Avm1Object<'_>]>),
}

impl Debug for ResponderHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponderHandle::Avm2(_) => write!(f, "ResponderHandle::Avm2"),
            ResponderHandle::Avm1(_) => write!(f, "ResponderHandle::Avm1"),
        }
    }
}

impl ResponderHandle {
    pub fn call(
        &self,
        context: &mut UpdateContext<'_>,
        callback: ResponderCallback,
        message: Rc<AmfValue>,
    ) {
        match self {
            ResponderHandle::Avm2(handle) => {
                let object = context.dynamic_root.fetch(handle);
                let mut activation = Avm2Activation::from_nothing(context);

                if let Err(err) = object.send_callback(&mut activation, callback, &message) {
                    Avm2::uncaught_error(
                        &mut activation,
                        None, // TODO we need to set this, but how?
                        err,
                        "Error running AVM2 NetConnection callback",
                    );
                }
            }
            ResponderHandle::Avm1(handle) => {
                let object = context.dynamic_root.fetch(handle);
                if let Err(e) =
                    Avm1NetConnectionObject::send_callback(context, *object, callback, &message)
                {
                    tracing::error!("Unhandled error sending {callback:?} callback: {e}");
                }
            }
        }
    }
}

#[derive(Copy, Clone, Collect)]
#[collect(no_drop)]
pub enum NetConnectionObject<'gc> {
    Avm2(Avm2NetConnectionObject<'gc>),
    Avm1(Avm1Object<'gc>),
}

impl NetConnectionObject<'_> {
    pub fn set_handle(&self, handle: Option<NetConnectionHandle>) -> Option<NetConnectionHandle> {
        match self {
            NetConnectionObject::Avm2(object) => object.set_handle(handle),
            NetConnectionObject::Avm1(object) => {
                if let Some(net_connection) = Avm1NetConnectionObject::cast((*object).into()) {
                    net_connection.set_handle(handle)
                } else {
                    None
                }
            }
        }
    }
}

impl<'gc> From<Avm2NetConnectionObject<'gc>> for NetConnectionObject<'gc> {
    fn from(value: Avm2NetConnectionObject<'gc>) -> Self {
        NetConnectionObject::Avm2(value)
    }
}

impl<'gc> From<Avm1Object<'gc>> for NetConnectionObject<'gc> {
    fn from(value: Avm1Object<'gc>) -> Self {
        NetConnectionObject::Avm1(value)
    }
}

/// Manages the collection of NetConnections.
#[derive(Collect)]
#[collect(no_drop)]
pub struct NetConnections<'gc> {
    connections: SlotMap<NetConnectionHandle, NetConnection<'gc>>,
}

impl Default for NetConnections<'_> {
    fn default() -> Self {
        Self {
            connections: SlotMap::with_key(),
        }
    }
}

impl<'gc> NetConnections<'gc> {
    pub fn connect_to_local<O: Into<NetConnectionObject<'gc>>>(
        context: &mut UpdateContext<'gc>,
        target: O,
    ) {
        let target = target.into();
        let connection = NetConnection {
            object: target,
            protocol: NetConnectionProtocol::Local,
        };
        let handle = context.net_connections.connections.insert(connection);

        if let Some(existing_handle) = target.set_handle(Some(handle)) {
            NetConnections::close(context, existing_handle, false);
        }

        match target {
            NetConnectionObject::Avm2(object) => {
                let mut activation = Avm2Activation::from_nothing(context);
                let event = Avm2EventObject::net_status_event(
                    &mut activation,
                    [
                        ("code", "NetConnection.Connect.Success"),
                        ("level", "status"),
                    ],
                );
                Avm2::dispatch_event(activation.context, event, object.into());
            }
            NetConnectionObject::Avm1(object) => {
                if let Err(e) = Avm1NetConnectionObject::on_status_event(
                    context,
                    object,
                    "NetConnection.Connect.Success",
                ) {
                    tracing::error!("Unhandled error sending connection callback: {e}");
                }
            }
        }
    }

    pub fn connect_to_flash_remoting<O: Into<NetConnectionObject<'gc>>>(
        context: &mut UpdateContext<'gc>,
        target: O,
        url: String,
    ) {
        let target = target.into();
        let connection = NetConnection {
            object: target,
            protocol: NetConnectionProtocol::FlashRemoting(FlashRemoting {
                url,
                headers: vec![],
                outgoing_queue: vec![],
            }),
        };
        let handle = context.net_connections.connections.insert(connection);

        if let Some(existing_handle) = target.set_handle(Some(handle)) {
            NetConnections::close(context, existing_handle, false);
        }

        // No open event here
    }

    pub fn connect_to_rtmp<O: Into<NetConnectionObject<'gc>>>(
        context: &mut UpdateContext<'gc>,
        target: O,
        url: String,
        arguments: Vec<AmfValue>,
    ) -> Result<(), String> {
        let target = target.into();
        let rtmp_connection = RtmpConnection::new(url, arguments)?;
        let host = rtmp_connection.host().to_string();
        let port = rtmp_connection.port();
        let connection = NetConnection {
            object: target,
            protocol: NetConnectionProtocol::Rtmp(Box::new(Rtmp {
                connection: rtmp_connection,
                socket: None,
                socket_events: VecDeque::new(),
                outgoing_queue: VecDeque::new(),
                shared_object_queue: VecDeque::new(),
                responders: BTreeMap::new(),
                next_transaction_id: 2,
            })),
        };
        let handle = context.net_connections.connections.insert(connection);

        if let Some(existing_handle) = target.set_handle(Some(handle)) {
            NetConnections::close(context, existing_handle, false);
        }

        let socket = context
            .sockets
            .connect_rtmp(context.navigator, handle, host, port);
        if let Some(NetConnection {
            protocol: NetConnectionProtocol::Rtmp(rtmp),
            ..
        }) = context.net_connections.connections.get_mut(handle)
        {
            rtmp.socket = Some(socket);
        }
        Ok(())
    }

    pub fn close(context: &mut UpdateContext<'gc>, handle: NetConnectionHandle, is_explicit: bool) {
        let Some(connection) = context.net_connections.connections.remove(handle) else {
            return;
        };
        let is_rtmp = matches!(&connection.protocol, NetConnectionProtocol::Rtmp(_));
        if let NetConnectionProtocol::Rtmp(rtmp) = &connection.protocol
            && let Some(socket) = rtmp.socket
        {
            context.sockets.close(socket);
        }

        match connection.object {
            NetConnectionObject::Avm2(object) => {
                let mut activation = Avm2Activation::from_nothing(context);
                let event = Avm2EventObject::net_status_event(
                    &mut activation,
                    [
                        ("code", "NetConnection.Connect.Closed"),
                        ("level", "status"),
                    ],
                );
                Avm2::dispatch_event(activation.context, event, object.into());

                if is_explicit
                    && matches!(connection.protocol, NetConnectionProtocol::FlashRemoting(_))
                {
                    // [NA] I have no idea why, but a NetConnection receives a second and nonsensical event on close
                    let event = Avm2EventObject::net_status_event(
                        &mut activation,
                        [
                            ("code", ""),
                            ("description", ""),
                            ("details", ""),
                            ("level", "status"),
                        ],
                    );
                    Avm2::dispatch_event(activation.context, event, object.into());
                }
            }
            NetConnectionObject::Avm1(object) => {
                if let Err(e) = Avm1NetConnectionObject::on_status_event(
                    context,
                    object,
                    "NetConnection.Connect.Closed",
                ) {
                    tracing::error!("Unhandled error sending connection callback: {e}");
                }
                if is_explicit
                    && matches!(connection.protocol, NetConnectionProtocol::FlashRemoting(_))
                    && let Err(e) = Avm1NetConnectionObject::on_empty_status_event(context, object)
                {
                    tracing::error!("Unhandled error sending connection callback: {e}");
                }
            }
        }
        if is_rtmp {
            close_remote_shared_objects(context, handle);
        }
    }

    pub fn update_connections(context: &mut UpdateContext<'gc>) {
        let player = context.player_handle();
        let mut rtmp_events = Vec::new();
        for (handle, connection) in context.net_connections.connections.iter_mut() {
            let events = connection.update(handle, context.navigator, &player, context.sockets);
            rtmp_events.extend(
                events
                    .into_iter()
                    .map(|event| (handle, connection.object, event)),
            );
        }
        for (handle, object, event) in rtmp_events {
            dispatch_rtmp_event(context, handle, object, event);
        }
    }

    pub(crate) fn queue_rtmp_socket_event(
        &mut self,
        handle: NetConnectionHandle,
        event: RtmpSocketEvent,
    ) {
        if let Some(NetConnection {
            protocol: NetConnectionProtocol::Rtmp(rtmp),
            ..
        }) = self.connections.get_mut(handle)
        {
            rtmp.socket_events.push_back(event);
        }
    }

    pub fn send_without_response(
        context: &mut UpdateContext<'gc>,
        handle: NetConnectionHandle,
        command: String,
        message: AmfValue,
    ) {
        if let Some(connection) = context.net_connections.connections.get_mut(handle) {
            connection.send(command, None, message);
        }
    }

    pub fn use_remote_shared_object(
        context: &mut UpdateContext<'gc>,
        handle: NetConnectionHandle,
        name: String,
    ) -> bool {
        let Some(NetConnection {
            protocol: NetConnectionProtocol::Rtmp(rtmp),
            ..
        }) = context.net_connections.connections.get_mut(handle)
        else {
            return false;
        };
        rtmp.shared_object_queue
            .push_back(RtmpSharedObjectRequest::Event {
                name,
                event_type: 1,
            });
        true
    }

    pub fn queue_remote_shared_object_event(
        context: &mut UpdateContext<'gc>,
        handle: NetConnectionHandle,
        name: String,
        event_type: u8,
    ) -> bool {
        let Some(NetConnection {
            protocol: NetConnectionProtocol::Rtmp(rtmp),
            ..
        }) = context.net_connections.connections.get_mut(handle)
        else {
            return false;
        };
        rtmp.shared_object_queue
            .push_back(RtmpSharedObjectRequest::Event { name, event_type });
        true
    }

    pub fn send_remote_shared_object(
        context: &mut UpdateContext<'gc>,
        handle: NetConnectionHandle,
        name: String,
        method: String,
        arguments: Vec<AmfValue>,
    ) -> bool {
        let Some(NetConnection {
            protocol: NetConnectionProtocol::Rtmp(rtmp),
            ..
        }) = context.net_connections.connections.get_mut(handle)
        else {
            return false;
        };
        rtmp.shared_object_queue
            .push_back(RtmpSharedObjectRequest::Send {
                name,
                method,
                arguments,
            });
        true
    }

    pub fn send_avm2(
        context: &mut UpdateContext<'gc>,
        handle: NetConnectionHandle,
        command: String,
        message: AmfValue,
        responder: Avm2ResponderObject<'gc>,
    ) {
        let mc = context.gc();
        if let Some(connection) = context.net_connections.connections.get_mut(handle) {
            // TODO(moulins): it'd be nice to avoid the double indirection here...
            let responder_handle =
                ResponderHandle::Avm2(context.dynamic_root.stash(mc, Gc::new(mc, responder)));
            connection.send(command, Some(responder_handle), message);
        }
    }

    pub fn send_avm1(
        context: &mut UpdateContext<'gc>,
        handle: NetConnectionHandle,
        command: String,
        message: AmfValue,
        responder: Avm1Object<'gc>,
    ) {
        let mc = context.gc();
        if let Some(connection) = context.net_connections.connections.get_mut(handle) {
            // TODO(moulins): it'd be nice to avoid the double indirection here...
            let responder_handle =
                ResponderHandle::Avm1(context.dynamic_root.stash(mc, Gc::new(mc, responder)));
            connection.send(command, Some(responder_handle), message);
        }
    }

    pub fn set_header(&mut self, handle: NetConnectionHandle, header: Header) {
        if let Some(connection) = self.connections.get_mut(handle) {
            connection.set_header(header);
        }
    }

    pub fn is_connected(&self, handle: NetConnectionHandle) -> bool {
        self.connections
            .get(handle)
            .map(|c| c.is_connected())
            .unwrap_or_default()
    }

    pub fn get_connected_proxy_type(&self, handle: NetConnectionHandle) -> Option<&'static str> {
        self.connections
            .get(handle)
            .and_then(|c| c.connected_proxy_type())
    }

    pub fn get_far_id(&self, handle: NetConnectionHandle) -> Option<&'static str> {
        self.connections.get(handle).and_then(|c| c.far_id())
    }

    pub fn get_far_nonce(&self, handle: NetConnectionHandle) -> Option<&'static str> {
        self.connections.get(handle).and_then(|c| c.far_nonce())
    }

    pub fn get_near_id(&self, handle: NetConnectionHandle) -> Option<&'static str> {
        self.connections.get(handle).and_then(|c| c.near_id())
    }

    pub fn get_near_nonce(&self, handle: NetConnectionHandle) -> Option<&'static str> {
        self.connections.get(handle).and_then(|c| c.near_nonce())
    }

    pub fn get_protocol(&self, handle: NetConnectionHandle) -> Option<&'static str> {
        self.connections.get(handle).and_then(|c| c.protocol())
    }

    pub fn get_uri(&self, handle: NetConnectionHandle) -> Option<String> {
        self.connections.get(handle).and_then(|c| c.uri())
    }

    pub fn is_using_tls(&self, handle: NetConnectionHandle) -> Option<bool> {
        self.connections.get(handle).and_then(|c| c.using_tls())
    }
}

#[derive(Collect)]
#[collect(no_drop)]
pub struct NetConnection<'gc> {
    object: NetConnectionObject<'gc>,

    #[collect(require_static)]
    protocol: NetConnectionProtocol,
}

impl NetConnection<'_> {
    pub fn is_connected(&self) -> bool {
        match &self.protocol {
            NetConnectionProtocol::Local => true,
            NetConnectionProtocol::FlashRemoting(_) => false,
            NetConnectionProtocol::Rtmp(rtmp) => rtmp.connection.is_connected(),
        }
    }

    pub fn connected_proxy_type(&self) -> Option<&'static str> {
        match self.protocol {
            NetConnectionProtocol::Local | NetConnectionProtocol::Rtmp(_) => Some("none"),
            NetConnectionProtocol::FlashRemoting(_) => None,
        }
    }

    pub fn far_id(&self) -> Option<&'static str> {
        match self.protocol {
            NetConnectionProtocol::Local | NetConnectionProtocol::Rtmp(_) => Some(""),
            NetConnectionProtocol::FlashRemoting(_) => None,
        }
    }

    pub fn far_nonce(&self) -> Option<&'static str> {
        match self.protocol {
            NetConnectionProtocol::Local => {
                Some("0000000000000000000000000000000000000000000000000000000000000000")
            }
            NetConnectionProtocol::FlashRemoting(_) | NetConnectionProtocol::Rtmp(_) => None,
        }
    }

    pub fn near_id(&self) -> Option<&'static str> {
        match self.protocol {
            NetConnectionProtocol::Local | NetConnectionProtocol::Rtmp(_) => Some(""),
            NetConnectionProtocol::FlashRemoting(_) => None,
        }
    }

    pub fn near_nonce(&self) -> Option<&'static str> {
        match self.protocol {
            NetConnectionProtocol::Local => {
                Some("0000000000000000000000000000000000000000000000000000000000000000")
            }
            NetConnectionProtocol::FlashRemoting(_) | NetConnectionProtocol::Rtmp(_) => None,
        }
    }

    pub fn protocol(&self) -> Option<&'static str> {
        match self.protocol {
            NetConnectionProtocol::Local | NetConnectionProtocol::Rtmp(_) => Some("rtmp"),
            NetConnectionProtocol::FlashRemoting(_) => None,
        }
    }

    pub fn uri(&self) -> Option<String> {
        match &self.protocol {
            NetConnectionProtocol::Local => Some("null".to_string()),
            NetConnectionProtocol::FlashRemoting(remoting) => Some(remoting.url.to_string()),
            NetConnectionProtocol::Rtmp(rtmp) => Some(rtmp.connection.uri().to_string()),
        }
    }

    pub fn using_tls(&self) -> Option<bool> {
        match &self.protocol {
            NetConnectionProtocol::Local | NetConnectionProtocol::Rtmp(_) => Some(false),
            NetConnectionProtocol::FlashRemoting(_) => None,
        }
    }

    pub fn send(
        &mut self,
        command: String,
        responder_handle: Option<ResponderHandle>,
        message: AmfValue,
    ) {
        match &mut self.protocol {
            NetConnectionProtocol::Local => {}
            NetConnectionProtocol::FlashRemoting(remoting) => {
                remoting.send(command, responder_handle, message)
            }
            NetConnectionProtocol::Rtmp(rtmp) => {
                rtmp.outgoing_queue
                    .push_back((command, responder_handle, message));
            }
        }
    }

    fn update(
        &mut self,
        self_handle: NetConnectionHandle,
        navigator: &mut dyn NavigatorBackend,
        player: &Arc<Mutex<Player>>,
        sockets: &mut Sockets<'_>,
    ) -> Vec<RtmpDispatch> {
        match &mut self.protocol {
            NetConnectionProtocol::Local => Vec::new(),
            NetConnectionProtocol::FlashRemoting(remoting) => {
                if remoting.has_pending_packet() {
                    navigator.spawn_future(remoting.flush_queue(self_handle, player.clone()));
                }
                Vec::new()
            }
            NetConnectionProtocol::Rtmp(rtmp) => rtmp.update(sockets),
        }
    }

    pub fn set_header(&mut self, header: Header) {
        match &mut self.protocol {
            NetConnectionProtocol::Local | NetConnectionProtocol::Rtmp(_) => {}
            NetConnectionProtocol::FlashRemoting(remoting) => remoting.set_header(header),
        }
    }
}

#[derive(Debug)]
pub enum NetConnectionProtocol {
    /// A "local" connection, caused by connecting to null
    Local,

    /// Flash Remoting protocol, caused by connecting to a `http://` address.
    FlashRemoting(FlashRemoting),

    /// Native RTMP carried by the platform socket backend.
    Rtmp(Box<Rtmp>),
}

#[derive(Debug)]
pub struct Rtmp {
    connection: RtmpConnection,
    socket: Option<SocketHandle>,
    socket_events: VecDeque<RtmpSocketEvent>,
    outgoing_queue: VecDeque<(String, Option<ResponderHandle>, AmfValue)>,
    shared_object_queue: VecDeque<RtmpSharedObjectRequest>,
    responders: BTreeMap<u64, ResponderHandle>,
    next_transaction_id: u64,
}

#[derive(Debug)]
enum RtmpSharedObjectRequest {
    Event {
        name: String,
        event_type: u8,
    },
    Send {
        name: String,
        method: String,
        arguments: Vec<AmfValue>,
    },
}

impl Rtmp {
    fn drain_unavailable_queues(&mut self) -> Vec<RtmpDispatch> {
        let mut events = Vec::with_capacity(self.outgoing_queue.len());
        while let Some((_command, responder, _message)) = self.outgoing_queue.pop_front() {
            drop(responder);
            events.push(RtmpDispatch::CallFailed(
                "RTMP connection is closed".to_string(),
            ));
        }
        self.shared_object_queue.clear();
        self.responders.clear();
        events
    }

    fn update(&mut self, sockets: &mut Sockets<'_>) -> Vec<RtmpDispatch> {
        let mut events = Vec::new();
        let Some(socket) = self.socket else {
            return self.drain_unavailable_queues();
        };
        let mut terminal = false;

        while let Some(event) = self.socket_events.pop_front() {
            match event {
                RtmpSocketEvent::Connected => {
                    sockets.send(socket, self.connection.socket_connected());
                }
                RtmpSocketEvent::Failed => {
                    sockets.close(socket);
                    self.socket = None;
                    self.connection
                        .socket_failed("RTMP socket connection failed".to_string());
                    terminal = true;
                    break;
                }
                RtmpSocketEvent::Closed => {
                    self.socket = None;
                    self.connection.socket_closed();
                    terminal = true;
                    break;
                }
                RtmpSocketEvent::Data(data) => match self.connection.receive(data) {
                    Ok(outbound) => {
                        for bytes in outbound {
                            sockets.send(socket, bytes);
                        }
                    }
                    Err(error) => {
                        tracing::warn!("RTMP protocol error: {error}");
                        sockets.close(socket);
                        self.socket = None;
                        terminal = true;
                        break;
                    }
                },
            }
        }

        for event in self.connection.take_events() {
            if matches!(
                event,
                RtmpEvent::Rejected | RtmpEvent::Closed | RtmpEvent::ProtocolError(_)
            ) {
                terminal = true;
            }
            match event {
                RtmpEvent::Result {
                    transaction_id,
                    value,
                } => {
                    if let Some(responder) = self.responders.remove(&transaction_id) {
                        events.push(RtmpDispatch::Responder {
                            responder,
                            callback: ResponderCallback::Result,
                            value: Rc::new(value),
                        });
                    }
                }
                RtmpEvent::Error {
                    transaction_id,
                    value,
                } => {
                    if let Some(responder) = self.responders.remove(&transaction_id) {
                        events.push(RtmpDispatch::Responder {
                            responder,
                            callback: ResponderCallback::Status,
                            value: Rc::new(value),
                        });
                    }
                }
                event => events.push(RtmpDispatch::Event(event)),
            }
        }
        if terminal {
            if self.socket.is_some() {
                sockets.close(socket);
                self.socket = None;
            }
            self.socket_events.clear();
            self.outgoing_queue.clear();
            self.shared_object_queue.clear();
            self.responders.clear();
            return events;
        }

        if self.connection.is_connected() {
            while let Some((command, responder, message)) = self.outgoing_queue.pop_front() {
                let transaction_id = if responder.is_some() {
                    self.next_transaction_id as f64
                } else {
                    0.0
                };
                match self
                    .connection
                    .send_command(&command, transaction_id, &message)
                {
                    Ok(bytes) => {
                        if let Some(responder) = responder {
                            self.responders.insert(self.next_transaction_id, responder);
                            self.next_transaction_id += 1;
                        }
                        sockets.send(socket, bytes);
                    }
                    Err(error) => {
                        drop(responder);
                        events.push(RtmpDispatch::CallFailed(error));
                    }
                }
            }
            while let Some(request) = self.shared_object_queue.pop_front() {
                let encoded = match request {
                    RtmpSharedObjectRequest::Event { name, event_type } => {
                        self.connection.send_shared_object_event(&name, event_type)
                    }
                    RtmpSharedObjectRequest::Send {
                        name,
                        method,
                        arguments,
                    } => self
                        .connection
                        .send_shared_object_message(&name, &method, &arguments),
                };
                match encoded {
                    Ok(bytes) => sockets.send(socket, bytes),
                    Err(error) => events.push(RtmpDispatch::CallFailed(error)),
                }
            }
        }
        events
    }
}

fn dispatch_rtmp_event<'gc>(
    context: &mut UpdateContext<'gc>,
    handle: NetConnectionHandle,
    object: NetConnectionObject<'gc>,
    event: RtmpDispatch,
) {
    let event = match event {
        RtmpDispatch::Event(event) => event,
        RtmpDispatch::CallFailed(error) => {
            tracing::warn!("Unable to send RTMP command: {error}");
            dispatch_rtmp_status(context, object, "NetConnection.Call.Failed", "error");
            return;
        }
        RtmpDispatch::Responder {
            responder,
            callback,
            value,
        } => {
            responder.call(context, callback, value);
            return;
        }
    };
    match event {
        RtmpEvent::Connected => {
            dispatch_rtmp_status(context, object, "NetConnection.Connect.Success", "status")
        }
        RtmpEvent::Rejected => {
            dispatch_rtmp_status(context, object, "NetConnection.Connect.Rejected", "error");
            close_remote_shared_objects(context, handle);
        }
        RtmpEvent::Closed => {
            dispatch_rtmp_status(context, object, "NetConnection.Connect.Closed", "status");
            close_remote_shared_objects(context, handle);
        }
        RtmpEvent::ProtocolError(error) => {
            tracing::warn!("RTMP connection failed: {error}");
            dispatch_rtmp_status(context, object, "NetConnection.Connect.Failed", "error");
            close_remote_shared_objects(context, handle);
        }
        RtmpEvent::Result { .. } | RtmpEvent::Error { .. } => unreachable!(),
        RtmpEvent::SharedObject(shared_object) => {
            dispatch_remote_shared_object(context, handle, shared_object);
        }
        RtmpEvent::Invoke {
            method,
            transaction_id,
            arguments,
        } => match object {
            NetConnectionObject::Avm2(object) => {
                let mut activation = Avm2Activation::from_nothing(context);
                let client_name = AvmString::new_utf8(activation.gc(), "client");
                let client = match Avm2Value::from(object)
                    .get_public_property(client_name, &mut activation)
                {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!("Unable to read RTMP NetConnection.client: {error:?}");
                        return;
                    }
                };
                let arguments = arguments
                    .iter()
                    .map(|argument| crate::avm2::amf::deserialize_value(&mut activation, argument))
                    .collect::<Result<Vec<_>, _>>();
                let Ok(arguments) = arguments else {
                    tracing::warn!("Unable to deserialize RTMP invoke arguments");
                    return;
                };
                let method = AvmString::new_utf8(activation.gc(), method);
                let response = match client.call_public_property(
                    method,
                    FunctionArgs::from_slice(&arguments),
                    &mut activation,
                ) {
                    Ok(value) if transaction_id != 0.0 => Some(serialize_value(
                        &mut activation,
                        value,
                        AMFVersion::AMF0,
                        &mut Default::default(),
                    )),
                    Ok(_) => None,
                    Err(error) => {
                        Avm2::uncaught_error(
                            &mut activation,
                            None,
                            error,
                            "Error running AVM2 RTMP invoke",
                        );
                        None
                    }
                };
                drop(activation);
                if let Some(response) = response {
                    send_rtmp_response(context, handle, transaction_id, response);
                }
            }
            NetConnectionObject::Avm1(_) => {
                tracing::warn!("Server invokes over RTMP are not yet implemented for AVM1");
            }
        },
    }
}

fn close_remote_shared_objects<'gc>(context: &mut UpdateContext<'gc>, handle: NetConnectionHandle) {
    let targets = context
        .avm2_shared_objects
        .values()
        .copied()
        .filter(|object| object.is_remote() && object.connection() == Some(handle))
        .collect::<Vec<_>>();
    for target in targets {
        target.set_connection(None);
    }
}

fn dispatch_remote_shared_object<'gc>(
    context: &mut UpdateContext<'gc>,
    handle: NetConnectionHandle,
    shared_object: crate::rtmp::RtmpSharedObject,
) {
    tracing::debug!(
        name = %shared_object.name,
        version = shared_object.version,
        flags = ?shared_object.flags,
        "Dispatching RTMP SharedObject message"
    );
    let target = context
        .avm2_shared_objects
        .values()
        .copied()
        .find(|object| {
            object.is_remote()
                && object.connection() == Some(handle)
                && object.name() == &shared_object.name
        });
    let Some(target) = target else {
        tracing::warn!(
            "Ignoring RTMP SharedObject update for unknown object {:?}",
            shared_object.name
        );
        return;
    };

    let mut changes = Vec::new();
    for event in shared_object.events {
        match event {
            RtmpSharedObjectEvent::UseSuccess => {
                dispatch_shared_object_status(
                    context,
                    target,
                    "SharedObject.Connect.Success",
                    "status",
                );
            }
            RtmpSharedObjectEvent::Clear => {
                target.reset_data(context);
                changes.push(("clear", None));
            }
            RtmpSharedObjectEvent::Change { name, value } => {
                let mut activation = Avm2Activation::from_nothing(context);
                let value = crate::avm2::amf::deserialize_value(&mut activation, &value);
                let name = AvmString::new_utf8(activation.gc(), name);
                if let Ok(value) = value
                    && let Err(error) = Avm2Value::from(target.data()).set_public_property(
                        name,
                        value,
                        &mut activation,
                    )
                {
                    tracing::warn!("Unable to update remote SharedObject.data: {error:?}");
                }
                drop(activation);
                changes.push(("change", Some(name.to_string())));
            }
            RtmpSharedObjectEvent::Remove { name } => {
                let mut activation = Avm2Activation::from_nothing(context);
                let name = AvmString::new_utf8(activation.gc(), name);
                let multiname = Multiname::new(activation.avm2().find_public_namespace(), name);
                if let Err(error) =
                    Avm2Value::from(target.data()).delete_property(&mut activation, &multiname)
                {
                    tracing::warn!("Unable to remove remote SharedObject.data: {error:?}");
                }
                drop(activation);
                changes.push(("delete", Some(name.to_string())));
            }
            RtmpSharedObjectEvent::Raw { event_type, data } => tracing::debug!(
                event_type,
                length = data.len(),
                "Ignoring unsupported RTMP SharedObject event"
            ),
            RtmpSharedObjectEvent::SendMessage { method, arguments } => {
                let mut activation = Avm2Activation::from_nothing(context);
                let client_name = AvmString::new_utf8(activation.gc(), "client");
                let client = match Avm2Value::from(target)
                    .get_public_property(client_name, &mut activation)
                {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!("Unable to read remote SharedObject.client: {error:?}");
                        continue;
                    }
                };
                let arguments = arguments
                    .iter()
                    .map(|argument| crate::avm2::amf::deserialize_value(&mut activation, argument))
                    .collect::<Result<Vec<_>, _>>();
                let Ok(arguments) = arguments else {
                    tracing::warn!("Unable to deserialize remote SharedObject arguments");
                    continue;
                };
                let method = AvmString::new_utf8(activation.gc(), method);
                if let Err(error) = client.call_public_property(
                    method,
                    FunctionArgs::from_slice(&arguments),
                    &mut activation,
                ) {
                    Avm2::uncaught_error(
                        &mut activation,
                        None,
                        error,
                        "Error running remote SharedObject callback",
                    );
                }
            }
        }
    }
    if !changes.is_empty() {
        dispatch_shared_object_sync(context, target, changes);
    }
}

fn dispatch_shared_object_status<'gc>(
    context: &mut UpdateContext<'gc>,
    object: Avm2SharedObjectObject<'gc>,
    code: &'static str,
    level: &'static str,
) {
    let mut activation = Avm2Activation::from_nothing(context);
    let event =
        Avm2EventObject::net_status_event(&mut activation, [("code", code), ("level", level)]);
    Avm2::dispatch_event(activation.context, event, object.into());
}

fn dispatch_shared_object_sync<'gc>(
    context: &mut UpdateContext<'gc>,
    object: Avm2SharedObjectObject<'gc>,
    changes: Vec<(&'static str, Option<String>)>,
) {
    let mut activation = Avm2Activation::from_nothing(context);
    let mut change_values = Vec::with_capacity(changes.len());
    for (code, name) in changes {
        let descriptor = crate::avm2::object::ScriptObject::new_object(activation.context);
        let code_name = AvmString::new_utf8(activation.gc(), "code");
        let code = AvmString::new_utf8(activation.gc(), code);
        let _ = Avm2Value::from(descriptor).set_public_property(
            code_name,
            code.into(),
            &mut activation,
        );
        if let Some(name) = name {
            let property_name = AvmString::new_utf8(activation.gc(), "name");
            let name = AvmString::new_utf8(activation.gc(), name);
            let _ = Avm2Value::from(descriptor).set_public_property(
                property_name,
                name.into(),
                &mut activation,
            );
        }
        change_values.push(Avm2Value::from(descriptor));
    }
    let change_list =
        Avm2ArrayObject::from_storage(activation.context, change_values.into_iter().collect());
    let sync_event = activation.avm2().classes().syncevent;
    let event_name = AvmString::new_utf8(activation.gc(), "sync");
    let event = Avm2EventObject::from_class_and_args(
        &mut activation,
        sync_event,
        &[
            event_name.into(),
            false.into(),
            false.into(),
            change_list.into(),
        ],
    );
    Avm2::dispatch_event(activation.context, event, object.into());
}

fn send_rtmp_response(
    context: &mut UpdateContext<'_>,
    handle: NetConnectionHandle,
    transaction_id: f64,
    value: AmfValue,
) {
    let Some(NetConnection {
        protocol: NetConnectionProtocol::Rtmp(rtmp),
        ..
    }) = context.net_connections.connections.get_mut(handle)
    else {
        return;
    };
    let Some(socket) = rtmp.socket else {
        return;
    };
    let arguments = AmfValue::StrictArray(ObjectId::INVALID, vec![Rc::new(value)]);
    match rtmp
        .connection
        .send_command("_result", transaction_id, &arguments)
    {
        Ok(bytes) => context.sockets.send(socket, bytes),
        Err(error) => tracing::warn!("Unable to encode RTMP invoke response: {error}"),
    }
}

fn dispatch_rtmp_status<'gc>(
    context: &mut UpdateContext<'gc>,
    object: NetConnectionObject<'gc>,
    code: &'static str,
    level: &'static str,
) {
    match object {
        NetConnectionObject::Avm2(object) => {
            let mut activation = Avm2Activation::from_nothing(context);
            let event = Avm2EventObject::net_status_event(
                &mut activation,
                [("code", code), ("level", level)],
            );
            Avm2::dispatch_event(activation.context, event, object.into());
        }
        NetConnectionObject::Avm1(object) => {
            if let Err(error) = Avm1NetConnectionObject::on_status_event(context, object, code) {
                tracing::error!("Unhandled error sending RTMP status callback: {error}");
            }
        }
    }
}

#[derive(Debug)]
pub struct FlashRemoting {
    url: String,
    headers: Vec<Header>,
    outgoing_queue: Vec<(Message, Option<ResponderHandle>)>,
}

impl FlashRemoting {
    pub fn send(
        &mut self,
        command: String,
        responder_handle: Option<ResponderHandle>,
        message: AmfValue,
    ) {
        self.outgoing_queue.push((
            Message {
                target_uri: command,
                response_uri: format!("/{}", self.outgoing_queue.len() + 1), // Flash is 1-based... simplifies tests to stay the same
                contents: Rc::new(message),
            },
            responder_handle,
        ));
    }

    pub fn has_pending_packet(&self) -> bool {
        !self.outgoing_queue.is_empty()
    }

    pub fn set_header(&mut self, header: Header) {
        // Only one header of the same name (case insensitive) should exist
        self.headers
            .retain(|h| !h.name.eq_ignore_ascii_case(&header.name));

        self.headers.push(header);
    }

    pub fn flush_queue(
        &mut self,
        self_handle: NetConnectionHandle,
        player: Arc<Mutex<Player>>,
    ) -> OwnedFuture<(), Error> {
        let queue = std::mem::take(&mut self.outgoing_queue);
        let (messages, responder_handles): (Vec<_>, Vec<_>) = queue.into_iter().unzip();
        let packet = Packet {
            version: AMFVersion::AMF0,
            headers: self.headers.clone(),
            messages,
        };
        let url = self.url.clone();

        Box::pin(async move {
            let bytes = flash_lso::packet::write::write_to_bytes(&packet, true)
                .expect("Must be able to serialize a packet");
            let request = Request::post(url, Some((bytes, "application/x-amf".to_string())));
            let fetch = player.lock().unwrap().fetch(request, FetchReason::Other);
            let response: Result<_, ErrorResponse> = async {
                let response = fetch.await?;
                let url = response.url().to_string();
                let body = response
                    .body()
                    .await
                    .map_err(|error| ErrorResponse { url, error })?;

                Ok(body)
            }
            .await;
            let response = match response {
                Ok(response) => response,
                Err(response) => {
                    player.lock().unwrap().update(|uc| {
                        tracing::error!(
                            "Couldn't submit AMF Packet to {}: {:?}",
                            response.url,
                            response.error
                        );
                        if let Some(connection) = uc.net_connections.connections.get(self_handle) {
                            match connection.object {
                                NetConnectionObject::Avm2(object) => {
                                    let mut activation = Avm2Activation::from_nothing(uc);
                                    let event = Avm2EventObject::net_status_event(
                                        &mut activation,
                                        [
                                            ("code", "NetConnection.Call.Failed"),
                                            ("level", "error"),
                                            ("details", &response.url),
                                            ("description", "HTTP: Failed"),
                                        ],
                                    );
                                    Avm2::dispatch_event(activation.context, event, object.into());
                                }
                                NetConnectionObject::Avm1(object) => {
                                    if let Err(e) =
                                        Avm1NetConnectionObject::on_empty_status_event(uc, object)
                                    {
                                        tracing::error!(
                                            "Unhandled error sending connection callback: {e}"
                                        );
                                    }
                                }
                            }
                        }
                    });
                    return Ok(());
                }
            };

            // Flash completely ignores invalid responses, it seems
            if let Ok(response_packet) = flash_lso::packet::read::parse(&response) {
                player.lock().unwrap().update(|uc| {
                    for message in response_packet.messages {
                        if let Some(target_uri) = message.target_uri.strip_prefix('/') {
                            let mut responder = None;
                            if let Some(index) = target_uri
                                .strip_suffix("/onStatus")
                                .and_then(|str| str::parse::<usize>(str).ok())
                            {
                                responder = responder_handles
                                    .get(index.wrapping_sub(1))
                                    .cloned()
                                    .flatten()
                                    .map(|handle| (handle, ResponderCallback::Status));
                            } else if let Some(index) = target_uri
                                .strip_suffix("/onResult")
                                .and_then(|str| str::parse::<usize>(str).ok())
                            {
                                responder = responder_handles
                                    .get(index.wrapping_sub(1))
                                    .cloned()
                                    .flatten()
                                    .map(|handle| (handle, ResponderCallback::Result));
                            }

                            if let Some((responder_handle, callback)) = responder {
                                responder_handle.call(uc, callback, message.contents);
                            }
                        }
                    }
                });
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_rtmp_drains_new_calls_and_shared_object_messages() {
        let mut rtmp = Rtmp {
            connection: RtmpConnection::new("rtmp://127.0.0.1/application".to_string(), Vec::new())
                .unwrap(),
            socket: None,
            socket_events: VecDeque::new(),
            outgoing_queue: VecDeque::from([("lateCall".to_string(), None, AmfValue::Null)]),
            shared_object_queue: VecDeque::from([RtmpSharedObjectRequest::Event {
                name: "lateObject".to_string(),
                event_type: 1,
            }]),
            responders: BTreeMap::new(),
            next_transaction_id: 2,
        };

        let events = rtmp.drain_unavailable_queues();

        assert!(matches!(
            events.as_slice(),
            [RtmpDispatch::CallFailed(message)] if message == "RTMP connection is closed"
        ));
        assert!(rtmp.outgoing_queue.is_empty());
        assert!(rtmp.shared_object_queue.is_empty());
        assert!(rtmp.responders.is_empty());
    }
}
