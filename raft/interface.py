import zmq
import time
import multiprocessing
from queue import Empty

from .protocol import MessageType

class Talker(multiprocessing.Process):
	def __init__(self, identity):
		super(Talker, self).__init__()

		# Port to talk from
		self.address = identity['my_id']

		# Backoff amounts
		self.initial_backoff = 1.0
		self.operation_backoff = 0.0001

		# Place to store outgoing messages
		self.messages = multiprocessing.Queue()

		# Signals
		self._ready_event = multiprocessing.Event()
		self._stop_event = multiprocessing.Event()

	def stop(self):
		self._stop_event.set()
		# pub_socket.unbind("tcp://%s" % self.address)
		# pub_socket.close()

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		# print("IN RUN OF PUB")
		context = zmq.Context()
		# print("Got context")
		pub_socket = context.socket(zmq.PUB)
		# print("Got socket")
		while True:
			# print("In while")
			try:
				#print("Trying for: ", self.address)
				pub_socket.bind("tcp://%s" % self.address)
				# print("trying")
				break
			except zmq.ZMQError as e:
				print(e)
				# time.sleep(0.1)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		# Signal that you're ready
		self._ready_event.set()
		# self._ready_event.set()
		# print(self._stop_event.is_set())
		while not self._stop_event.is_set():
			try:
				#print("IN PUB WHILE LOOP")
				msg = self.messages.get_nowait()
				# if(msg['type'] == 4):
					# print("[PUB] Got message on my own queue: ", str(msg))
				pub_socket.send_json(msg)
			except Empty:
				try:
					time.sleep(self.operation_backoff)
				except KeyboardInterrupt:
					break
			except KeyboardInterrupt:
				pub_socket.unbind("tcp://%s" % self.address)
				pub_socket.close()
				break
		
		pub_socket.unbind("tcp://%s" % self.address)
		pub_socket.close()

	def send_message(self, msg):
		# self.send(msg)
		self.messages.put(msg)
		# if(msg['type'] == 4):
		# 	print("PUTTING MSG TO QUEUE: ", msg)
		# print(self.messages.get_nowait())
	
	def wait_until_ready(self):
		while not self._ready_event.is_set():
			time.sleep(0.1)
		return True

class Listener(multiprocessing.Process):
	def __init__(self, port_list, identity):
		super(Listener, self).__init__()

		# List of ports to subscribe to
		self.address_list = port_list
		self.identity = identity

		# Backoff amounts
		self.initial_backoff = 1.0

		# Place to store incoming messages
		self.messages = multiprocessing.Queue()

		# Signals
		self._stop_event = multiprocessing.Event()

	def stop(self):
		self._stop_event.set()
		# pub_socket.unbind("tcp://%s" % self.address)
		# sub_socket.close()

	def run(self):
		# All of the zmq initialization has to be in the same function for some reason
		# print("IN RUN")
		context = zmq.Context()
		sub_sock = context.socket(zmq.SUB)
		sub_sock.setsockopt(zmq.SUBSCRIBE, b'')
		for a in self.address_list:
			# print(a)
			sub_sock.connect("tcp://%s" % a)

		# Poller lets you specify a timeout
		poller = zmq.Poller()
		poller.register(sub_sock, zmq.POLLIN)

		# Need to backoff to give the connections time to initizalize
		time.sleep(self.initial_backoff)

		while not self._stop_event.is_set():
			#print("IN WHILE OF RUN")
			try:
				obj = dict(poller.poll(100))
				# if sub_sock in obj and obj[sub_sock] == zmq.POLLIN:
				# print("IN IF")
				msg = sub_sock.recv_json()	
				if ((msg['receiver'] == self.identity['my_id']) or (msg['receiver'] is None)):
					#print("In run()")
					# if(msg['type'] == 4):
					# 	print("[PUB] Got message on my own queue: ", str(self.identity['my_id']), str(msg))
					self.messages.put(msg)
			except KeyboardInterrupt:
				sub_sock.close()
				break
		
		sub_sock.close()
	
	def get_message(self):
		# If there's nothing in the queue Queue.Empty will be thrown
		try:
			msg = self.messages.get_nowait()
			# if(msg['type'] == 4):
			# 	print("Read msg from queue: ", msg)
			return msg
		except Empty:
			return None