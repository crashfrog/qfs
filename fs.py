import sys
import json
import stat
import errno
import requests
import logging
from io import BytesIO
from os import environ, mkfifo, sep, unlink
from os.path import join as j, relpath as r, split
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from subprocess import Popen
from collections import OrderedDict
from time import time

DIRECTORY = 0o040000
FILE	  = 0o100000
RWXR_XR_X = 0o000755

# example = {'n':'/',  					#name
# 		   't':'d', 					#type
# 		   'c':[{'n':'child_dir',		#children
# 		   		 't':'d',
# 		   		 'c':[{'n':'child_file.txt',
# 					   't':'literal',
# 					   'd':["some JSONable stuff", 123, dict(key='value')],
# 					   'm':{'st_size':1024}}	#metadata
# 					 ],
# 				 'm':{}},
# 		   		{'n':'child_stream.zip',
# 		   		 't':'uri',
# 		   		 's':'http://ipv4.download.thinkbroadband.com/5MB.zip',
# 		   		 'm':{}},
# 		   		{'n':'child_process.fastq',
# 		   		 't':'process',
# 		   		 'f':'/fifo/fa',
# 		   		 'p':'fastq-dump -Z SRR000123',
# 		   		 'm':{}}
# 			   ],
# 			'm':{}
# 			}

def get(struct, mount_point=None):
	return types[struct['t']](mp=mount_point, **struct)

class Dir(LoggingMixIn, Operations):
	"Basic container"
	
	st_mode = (0o040000  #directory
			 | 0o000755 ) #rwxr-xr-x
	
	def __init__(self, n, mp=None, m={}, c=[], **k):
		self.mp = mp
		self.name = n
		self.childs = OrderedDict()
		self.metadata = m
		for r in c:
			s = get(r)
			self.childs[s.name] = s
			
	def __no_such__(self):
		raise OSError(errno.ENOENT)
			
	def __resolve__(self, path):
		#print(path)
		p = r(path, self.name)
		if p == '.':
			return self
		try:
			return self.childs[p.split(sep)[0]].__resolve__(p)
		except KeyError as e:
			#pass
			raise OSError(errno.ENOENT) from e
			#raise OSError(errno.EPERM) from e
		
			
	def __traverse__(self, prefix='/'):
		path = j(prefix, self.name)
		traverse = [path]
		for r in self.childs.values():
			traverse.extend(r.__traverse__(path))
		return traverse
		
			
	def __repr__(self):
		return "<{} object named '{}'>".format(type(self).__name__, self.name)
		
	# Private filesystem implementations #
	
	def __access__(self, mode):
		return 0
		
	def __chmod__(self, mode):
		return 0
		
	def __chown__(self, uid, gid):
		return 0
		
	def __stat__(self, fh):
		t = time()
		return dict(dict(st_atime=t,
						 st_ctime=t,
						 st_gid=1,
						 st_mode=self.st_mode,
						 st_nlink=2,
						 st_size=1024,
						 st_mtime=t),
					**self.metadata)
	
			
	def __readdir__(self, fh):
		yield from ['.', '..', ]
		yield from self.childs.keys()
		
	def __mknod__(self, mode, dev):
		return 0
		
	def __rmdir__(self, name):
		return 0
		
	def __mkdir__(self, name, mode):
		return 0
		
	def __statfs__(self):
		return dict(f_bavail=0,
					f_bfree=0,
					f_blocks=0,
					f_bsize=0,
					f_favail=0,
					f_ffree=0,
					f_files=0,
					f_flag=0,
					f_frsize=0,
					f_namemax=0)
					
	def __unlink__(self, name):
		return 0
		
	def __rename__(self, name):
		return 0
		
	def __link__(self, target, name):
		return 0
		
	def __utime__(self, times):
		return 0
		
	def __open__(self, flags):
		return 0
		
	def __create__(self, name, fi):
		return 0
	
	def __read__(self, length, offset, fh):
		return 0
		
	def __write__(self, buf, offset, fh):
		return 0
		
	def __truncate__(self, length, fh):
		return 0
		
	def __flush__(self, fh):
		return 0
		
	def __close__(self, fh):
		return 0
		
	def __fsync__(fdatasync, fh):
		return 0
	
	
	# Public filesystem methods #
	
	def access(self, path, mode):
		return self.__resolve__(path).__access__(mode)
		
	def chmod(self, path, mode):
		return self.__resolve__(path).__chmod__(mode)
		
	def chown(self, path, uid, gid):
		return self.__resolve__(path).__chown__(uid, gid)
			
	def getattr(self, path, fh=None):
		return self.__resolve__(path).__stat__(fh)
	
	def readdir(self, path, fh):
		return self.__resolve__(path).__readdir__(fh)
		
	def readlink(self, path):
		return False

	def mknod(self, path, mode, dev):
		return self.__resolve__(path).__mknod__(mode, dev)
		
	def rmdir(self, path):
		path, name = split(path)
		return self.__resolve__(path).__rmdir__(name)
		
	def mkdir(self, path, mode):
		path, name = split(path)
		return self.__resolve__(path).__mkdir__(name, mode)
		
	def statfs(self, path):
		return self.__resolve__(path).__statfs__()
		
	def unlink(self, path):
		path, name = split(path)
		return self.__resolve__(path).__unlink__(name)
		
	def rename(self, path, new):
		return self.__resolve__(path).__rename__(new)
		
	def link(self, target, name):
		path, name = split(name)
		return self.__resolve__(path).__link__(target, name)
		
	def utimens(self, path, times=None):
		return self.__resolve__(path).__utime__(times)
		
	def open(self, path, flags):
		return self.__resolve__(path).__open__(flags)
		
	def create(self, path, mode, fi=None):
		path, name = split(name)
		return self.__resolve__(path).__create__(name, fi)
		
	def read(self, path, length, offset, fh):
		return self.__resolve__(path).__read__(length, offset, fh)
		
	def write(self, path, buf, offset, fh):
		return self.__resolve__(path).__write__(buf, offset, fh)
		
	def truncate(self, path, length, fh=None):
		return self.__resolve__(path).__truncate__(length, fh)
		
	def flush(self, path, fh):
		return self.__resolve__(path).__flush__(fh)
		
	def release(self, path, fh):
		return self.__resolve__(path).__close__(fh)
		
	def fsync(self, path, fdatasync, fh):
		return self.__resolve__(path).__fsync__(fdatasync, fh)
	

	
	
class WritableDir(Dir):

	def __init__(self, n, c, s, **k):
		super().__init__(n, c, **k)
		self.stream = s
	
	def __create__(self, mode, fp=None):
		return False
		
		
class Node(Dir):
	"filesystem node"
	
	st_mode = (0o100000		#file
			 | 0o000755	)	#rwxr-xr-x
	
	def __init__(self, **k):
		super().__init__(**k)
		

		
class JsonStream(Node):
	"JSON from a string buffer"
	
	def __init__(self, d, **k):
		super().__init__(**k)
		self.data = d
		
	def __open__(self, flags):
		self.buffer = BytesIO(str.encode(json.dumps(self.data)))
		return 0
		
	def __read__(self, length, offset, fh):
		self.buffer.seek(offset)
		return self.buffer.read(length)
		
	def __close__(self, fh):
		self.buffer.close()
		del self.buffer
		return 0
		
class Passthrough(Node):
	
	def __init__(self, f, **k):
		super().__init__(**k)
		self.fp = f
		
	def __open__(self, flags):
		self.fh = open(self.fp, 'rb')
		return 0
		
	def __read__(self, length, offset, fh):
		self.fh.seek(offset)
		return self.fh.read(length)
		
	def __close__(self, fh):
		self.fh.close()
		del self.fh
		
class Stream(Dir):
	"streams"
	
	st_mode = (0o100000 #file
			 | 0o755 )
			 
	def __read__(self, length, offset, fh):
		"stream - offset reads not permitted"
		return self.stream.read(length)

class UriStream(Stream):

	def __init__(self, s, **k):
		super().__init__(**k)
		self.s = s
	
	def __open__(self, flags):
		self.stream = requests.get(self.stream).raw
		return 0
		
	def __close__(self, fh):
		del self.request
		return 0
	
class ProcessStream(Stream, Passthrough):
	"pipe a process to a fifo"
	def __init__(self, p, **k):
		super().__init__(**k)
		self.process = p
		
	def __open__(self, flags):
		#set up the fifo
		mkfifo(self.fp)
		self.popen = Popen(self.process + '>' + self.fp, shell=True)
		self.stream = open(self.fp, 'rb')
		return 0
		
	def __close__(self, fh):
		#close the fifo
		self.popen.terminate()
		del self.popen
		self.stream.close()
		unlink(self.fp)
		return 0
	
		
		
		
types = {
		 'd':Dir,
		 'literal':JsonStream,
		 'uri':UriStream,
		 'file':Passthrough,
		 'process':ProcessStream
		 }
		 
if __name__ == '__main__':
	 # create the default logger used by the logging mixin
	logger = logging.getLogger('fuse.log-mixin')
	logger.setLevel(logging.DEBUG)
	# create console handler with a higher log level
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	# add the handlers to the logger
	logger.addHandler(ch)
# 	print('\n'.join(get(example).__traverse__()))
	with open(sys.argv[1], 'r') as struct:
		fs = get(json.load(struct))
# 		print('\n'.join(fs.__traverse__()))
# 		print(fs)
# 		print(fs.__resolve__('/'))
#		print(fs.__resolve__('/child_process.fastq'))
# 		print(fs.__resolve__('/child_dir/'))
# 		print(fs.__resolve__('/child_dir/child_file.txt'))
# 		print(fs.__resolve__('/doesnt_exist'))
#		print(fs.getattr('/child_dir'))
#		print(list(fs.readdir('/', None)))
		FUSE(fs, environ.get('MOUNT', '/data'), nothreads=True, foreground=True)