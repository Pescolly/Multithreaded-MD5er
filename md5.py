#!/usr/bin/python

import hashlib
import multiprocessing as mp
import sys
import os
import inspect
import argparse

DEBUG = False
MIN_BLOCKSIZE = 4 * 1024 * 1024
MAX_WORKER_COUNT = mp.cpu_count()

MD5_FILENAME = str()					# create md5 filename as global because

HASH_FILE_PAIRS = list()  				# list from incoming .md5 file

CREATE_MD5_FILE = False					# flags set from input arguments
CHECK_MD5 = False
FILE_MODE = False						# look at files only. ignore directories

WORKER_THREADS = list()						# list of all md5 processes
OUTPUT_THREADS = list()						# list of threads that output stuff
QUEUES = list()							# list of all queues

VERBOSE = False

#QUEUES.append(complete_md5_obj_queue)


class Logger(object):
	def __init__(s):
		pass

	def log(s, string):
		if DEBUG:
			s._print_msg(string)

	def _print_msg(s, string):
		my_stack = inspect.stack()[2][0]
		my_filename = my_stack.f_code.co_filename

		if 'self' in my_stack.f_locals.keys():
			class_val = my_stack.f_locals['self'].__class__
			my_class = '{0}.'.format(class_val.__name__)
		else:
			my_class = ''

		my_func = my_stack.f_code.co_name
		my_line = my_stack.f_lineno

		msg = '{0}: {1}{2}({3}): {4}'.format(
				my_filename,
				my_class,
				my_func,
				my_line,
				string)
		print msg


LOG = Logger()


class File(object):
	def __init__(s, fname, min_blksize=MIN_BLOCKSIZE):
		LOG.log('fname: {0}'.format(fname))
		s.fname       = fname
		s.min_blksize = min_blksize
		s.buffering   = False
		s.size        = 0
		s.blksize     = 0
		s.statdata    = None
		s.error       = False
		s.errors      = []
		s.fstream     = None
		s.i           = 0
		s.lstat()

	def lstat(s):
		LOG.log('fname: {0}'.format(s.fname))
		try:
			s.statdata = os.lstat(s.fname)
			s.size     = s.statdata.st_size

			t = s.statdata.st_blksize
			s.blksize = t if t >= s.min_blksize else s.min_blksize

		except OSError as e:
			#  cleanup and return
			s.error = True
			s.errors.append('File.lstat: {0} ({1})'.format(e.strerror, e.errno))
			raise

	def fopen(s):
		LOG.log('fname: {0}'.format(s.fname))
		try:
			buff = -1 if s.buffering else 0
			s.fstream = open(s.fname, 'rb', buff)

		except IOError as e:
			s.error = True
			s.errors.append('File.open(): {0} ({1})'.format(e.strerror, e.errno))
			raise

	def fclose(s):
		LOG.log('fname: {0}'.format(s.fname))
		s.fstream.close()


class FileIter(object):
	def __init__(s, fobj):
		s.fobj = fobj
		LOG.log('fname: {0}'.format(s.fobj.fname))
		s.fobj = fobj

	def __enter__(s):
		LOG.log('fname: {0}'.format(s.fobj.fname))
		s.fobj.fopen()
		return s

	def __exit__(s, type, value, traceback):
		LOG.log('fname: {0}'.format(s.fobj.fname))
		s.fobj.fclose()

	def __iter__(s):
		LOG.log('fname: {0}'.format(s.fobj.fname))
		return s

	def next(s):
		chunk = s.fobj.fstream.read(s.fobj.blksize)
		if len(chunk) == 0:
			raise StopIteration
		return chunk

	def close(s):
		s.fobj.fclose()


def cleanup_filename(in_str):
	if in_str.startswith("."):
		return in_str[2:]
	if in_str.startswith("/"):
		return in_str[1:]
	return in_str


class Md5(object):
	def __init__(s, in_work_q, check_md5_q, write_to_file_q):
		s.qwork = in_work_q
		s.qdata = mp.Queue(128)
		s.qcheckmd5 = check_md5_q
		s.qwrite_to_file = write_to_file_q
		s.hash_md5  = hashlib.md5()
		s.md5digest = None
		s.hash_complete = False
		s.readThread = None
		s.hashThread = None

	def start_threads(s):
		s.readThread = mp.Process(target=s.read_thread)
		WORKER_THREADS.append(s.readThread)
		s.hashThread = mp.Process(target=s.md5_thread)
		WORKER_THREADS.append(s.hashThread)
		s.readThread.start()
		s.hashThread.start()


	def read_thread(s):
		LOG.log('starting read thread')

		while True:
			in_file = s.qwork.get()
			if in_file == 'DONE':
				LOG.log("ending read thread")
				s.qdata.put(['END', None])
				return

			# file given
			if in_file is not None:
				LOG.log("got a file")
				s.qdata.put(['NEW', in_file])
				with FileIter(in_file) as f:
					for chunk in f:
						s.qdata.put(['DATA', chunk])
				LOG.log('Closing file')
				f.close()
				s.qdata.put(['DONE'])


	def md5_thread(s):
		LOG.log('Starting MD5')
		while True:
			hash_md5 = hashlib.md5()
			signal = s.qdata.get(True)
			if signal[0] == 'END':
				LOG.log('exiting')
				return

			elif signal[0] == 'NEW':
				fobj = signal[1]
				while True:
					data = s.qdata.get(True)
					if data[0] == 'DATA':
						hash_md5.update(data[1])

					elif data[0] == 'DONE':
						LOG.log('File Done')
						new_md5 = MD5HashFilenamePair(hash=hash_md5.hexdigest(), filepath=fobj.fname)

						# put complete md5 into appropriate qs
						if CHECK_MD5:
							s.qcheckmd5.put(new_md5)

						if CREATE_MD5_FILE:
							s.qwrite_to_file.put(new_md5)

						# return to main loop for work
						break

			else:
				LOG.log('whoah shit!!  {0} ({1})'.format(signal[0], len(signal[0])))
				return


	def terminate_threads(s):
		LOG.log("terminating Qs")
		s.qdata.close()
		if s.readThread is not None:
			if s.readThread.is_alive():
				LOG.log("Closing read thread")
				s.readThread.terminate()

		if s.hashThread is not None:
			if s.hashThread.is_alive():
				LOG.log("Closing hash thread")
				s.hashThread.terminate()

	@property
	def complete(s):
		return s.hash_complete

	def print_md5(s):
		return s.md5manifest()

	def md5manifest(s):
		return '{0}  {1}'.format(s.md5digest, s.file_name)


# contains hash and filepath from .MD5 files
class MD5HashFilenamePair(object):
	def __init__(s, line_from_md5_file=None, hash=None, filepath=None, file_mode=False):
		if line_from_md5_file is not None:
			s.HASH = line_from_md5_file.split(" ")[0].strip()
			if file_mode:
				s.FILEPATH = os.path.split(line_from_md5_file[33:])[1].strip()
			else:
				s.FILEPATH = line_from_md5_file[33:].strip()
		else:
			if hash is not None:
				s.HASH = hash
			if filepath is not None:
				s.FILEPATH = filepath


# parses .MD5 file and creates MD5_Hash_file_pair objects
class MD5FileParser:
	def __init__(s, in_md5file, file_mode=False):
		s.pairs = list()
		f = open(in_md5file)
		lines = tuple(f)
		for line in lines:
			s.pairs.append(MD5HashFilenamePair(line_from_md5_file=line, file_mode=file_mode))
		f.close()


# get all files in CWD
class Walker(object):
	def __init__(s, incoming_path):
		s.path = os.path.relpath(incoming_path)
		s.fileList = list()
		s.create_file_list()

	def create_file_list(s):
		for root, dirs, files in os.walk(s.path):
			for fyle in files:
				if fyle.startswith("."):
					continue

				filename = os.path.relpath(os.path.join(root, fyle), s.path)

				if filename.startswith("./"):
					filename = filename[2:]
				if filename.startswith("/"):
					filename = filename[1:]

				if filename in s.fileList:
					print "Filename duplicate: " + filename

				s.fileList.append(filename)


# loop runs and checks complete q
def compare_md5_run_loop(q_check_md5):
	while True:
		calculated_md5 = q_check_md5.get(True)

		if calculated_md5 is None:
			LOG.log("Breaking")
			break

		if isinstance(calculated_md5, MD5HashFilenamePair):
			match_found = False
			incorrect_checksum = False
			calculated_filename = calculated_md5.FILEPATH

			mismatched_pair = MD5HashFilenamePair()

			for existing_md5 in HASH_FILE_PAIRS:
				existing_filename = existing_md5.FILEPATH

				if FILE_MODE:  				# get filename only (ignore path)
					if os.path.split(existing_filename)[1] == os.path.split(calculated_filename)[1]:
						match_found = True
						if existing_md5.HASH.lower().strip() == calculated_md5.HASH.lower().strip():
							if VERBOSE:
								print "Found match: " + calculated_md5.FILEPATH
							break

						else:
							incorrect_checksum = True
							mismatched_pair.FILEPATH = existing_filename
							mismatched_pair.HASH = existing_md5.HASH
							continue

				else:
					if existing_filename == calculated_filename:
						match_found = True
						incorrect_checksum = False
						if existing_md5.HASH.lower().strip() == calculated_md5.HASH.lower().strip():
							if VERBOSE:
								print "Found match: " + calculated_md5.FILEPATH
							break

						else:
							incorrect_checksum = True
							mismatched_pair.FILEPATH = existing_filename
							mismatched_pair.HASH = existing_md5.HASH
							continue

			if incorrect_checksum:
				print "Checksums do not match:\n" + calculated_md5.FILEPATH + " AND " + mismatched_pair.FILEPATH
				print mismatched_pair.HASH + " " + calculated_md5.HASH

			if not match_found:
				print "No MD5 match found for file: " + calculated_md5.FILEPATH


# loop waits for hashes to come down complete md5 queue...
# and puts them into an .md5 file
def write_md5_to_file(incoming_q):
	f = open(MD5_FILENAME, 'w')

	while True:
		try:
			md5_obj = incoming_q.get(True)
			if md5_obj is not None:
				f.write(md5_obj.HASH + " " + md5_obj.FILEPATH + "\n")  	# write to file
				continue

			if md5_obj is None:
				break

		except os.error as e:
			print "Cannot Write to MD5 file"
			print e.strerror

	f.close()



def main(in_path=None, existing_hash=None):
	# test md5 file
	LOG.log("Creating dictionary of filenames and hashes from existing MD5")

	check_md5_queue = None
	write_to_file_queue = None

	if CHECK_MD5:
		# get existing hashes from file and put into list
		for pair in MD5FileParser(existing_hash, file_mode=FILE_MODE).pairs:
			HASH_FILE_PAIRS.append(pair)

		# start compare md5 process
		check_md5_queue = mp.Queue(1024)
		compare_md5_process = mp.Process(target=compare_md5_run_loop, args=(check_md5_queue,))
		compare_md5_process.start()

		#add shit to list
		QUEUES.append(check_md5_queue)
		OUTPUT_THREADS.append(compare_md5_process)


	#start md5 file writing process
	if CREATE_MD5_FILE:
		write_to_file_queue = mp.Queue(1024)
		md5_file_creation_process = mp.Process(target=write_md5_to_file, args=(write_to_file_queue,))
		md5_file_creation_process.start()

		QUEUES.append(write_to_file_queue)
		OUTPUT_THREADS.append(md5_file_creation_process)

	#create file list using walker
	w = Walker(in_path)

	#prelim check of existing md5s against file system.
	for fyle in HASH_FILE_PAIRS:
		try:
			if FILE_MODE:
				match_found = False
				for fileListObject in w.fileList:
					if os.path.split(fileListObject)[1].strip() == fyle.FILEPATH:
						match_found = True
						break
				if not match_found:
					print "File mode: Cannot find file to match MD5 for: " + fyle.FILEPATH

			else:
				if fyle.FILEPATH not in w.fileList:
					print "Cannot find file to match MD5 for: " + fyle.FILEPATH
		except:
			LOG.log("Error with file: " + fyle)


	# start work queue
	q_work = mp.Queue(1024)
	QUEUES.append(q_work)

#	def __init__(self, in_work_q, check_md5_q, write_to_file_q):
	# start md5 worker processes (reader thread)
	for core in range(MAX_WORKER_COUNT):
		md5_obj = Md5(q_work, check_md5_queue, write_to_file_queue)
		worker_proc = mp.Process(target=md5_obj.start_threads())
		worker_proc.start()
		WORKER_THREADS.append(worker_proc)

	#put file that the walker discovered into the work queue
	for fylepath in w.fileList:
		LOG.log("Putting fylepath in Queue: " + fylepath)
		q_work.put(File(fylepath))

	for core in range(MAX_WORKER_COUNT):
		q_work.put("DONE")

	for thread in WORKER_THREADS:
		LOG.log("Joining worker" + str(thread))
		thread.join()

	LOG.log("Closing down output queues")

	if CHECK_MD5:
		check_md5_queue.put(None)

	if CREATE_MD5_FILE:
		write_to_file_queue.put(None)

	LOG.log("Main is complete")
	print "DONE!"


# kill all threads
def terminate():
	for worker in WORKER_THREADS:
		if worker.is_alive():
			worker.terminate()
	for output in OUTPUT_THREADS:
		if output.is_alive():
			output.terminate()
	for q in QUEUES:
		q.close()

if __name__ == '__main__':
	try:
		parser = argparse.ArgumentParser(description="Multithreaded MD5 creation and verification")
		parser.add_argument("-g", help="Generate an MD5 file (Flag)", action="store_true")
		parser.add_argument("-d", help="specify Directory for MD5 creation")
		parser.add_argument("-c", help="Check MD5 file to verify against current directory")
		parser.add_argument("-o", help="specify Output for file MD5")
		parser.add_argument("-f", help="File mode. Ignores directories.", action="store_true")
		parser.add_argument("-v", help="Verbose mode. Prints found matches", action="store_true")
		args = parser.parse_args()

		if args.v is True:
			VERBOSE = True

		if args.g is False and args.c is None:
			print "-g or -c not flagged, nothing to do..."
			exit(1)

		if args.f is True:
			FILE_MODE = True

		# all work is done from target path
		target_dir = args.d
		if target_dir is not None:
			if not os.path.isabs(target_dir):
				if os.path.exists(target_dir):
					target_dir = os.path.join(os.getcwd(), cleanup_filename(target_dir))
					if not os.path.exists(target_dir):
						print "Cannot find path: " + target_dir
						exit(1)
			else:
				LOG.log("Not an absolute path")
		else:
			target_dir = os.getcwd()

		# check if specified file is legit
		existing_md5_file_path = args.c
		if existing_md5_file_path is not None:
			CHECK_MD5 = True
			print("Checking MD5 file: " + existing_md5_file_path)
			LOG.log("Check MD5 file set to TRUE")
			if not os.path.isabs(existing_md5_file_path):
				existing_md5_file_path = os.path.join(os.getcwd(), cleanup_filename(existing_md5_file_path))
				if not os.path.exists(existing_md5_file_path):
					print "MD5 file cannot be found: " + existing_md5_file_path
					exit(1)

		# change to target path
		os.chdir(target_dir)

		# flag to create an MD5 file
		if args.g:
			CREATE_MD5_FILE = True
			pathname = os.path.split(target_dir)[1]
			new_md5_filename = args.o
			if new_md5_filename is not None:			# specify new filename
				if os.path.isdir(new_md5_filename):
					MD5_FILENAME = os.path.join(new_md5_filename, os.path.split(target_dir)[1] + ".md5")
				else:
					if os.path.splitext(new_md5_filename)[1] != ".md5":
						MD5_FILENAME = new_md5_filename + ".md5"
					else:
						MD5_FILENAME = new_md5_filename
			else:											# use path for name
				MD5_FILENAME = os.path.join(os.getcwd(), os.path.split((os.getcwd()))[1] + ".md5")

			print "Creating file at: " + MD5_FILENAME


		LOG.log("Checking current working directory")
		if not os.path.exists(target_dir):
			print "Path does not exit: " + target_dir
			exit(1)

		LOG.log("Starting MD5 check")
		main(in_path=target_dir, existing_hash=existing_md5_file_path)

	except KeyboardInterrupt:
		print 'Ctl-C caught, exiting..'
		terminate()
		sys.exit(1)
