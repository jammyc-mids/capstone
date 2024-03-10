from rejson import Client, Path

class rejsonConnector:
   def __init__(self):
      self.redissrv = "localhost"
      self.port = "6379"
      self.driver = Client(host=self.redissrv, port=self.port, decode_responses=True)

   def getallkeys(self):
      klist = []
      try:
         klist = self.driver.keys('*')
      except:
         self.driver = None
      return klist

   def rejsonGet(self, key, path):
      return self.driver.jsonget(key, path)

   def rejsonSet(self, key, path, data):
      return self.driver.jsonset(key, path, data)

   def rejsonUpdate(self, key, path, udata):
      # to add new subkey in a level N+1
      # e.g. Add/update new irradiance data for a specific date 
      tdata = self.rejsonGet(key, path)
      tdata[uk] = udata[uk]
      self.rejsonSet(key, path, tdata)
