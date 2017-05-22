

class Utils(object):
    def __init__(self, act=None, entty=None, datacenter=None, log=None):
        self._action_name = act
        self.entt_type = entty
        self.datacenter = datacenter
        self.log = log
        self.log.debug('Utils initialized')

    def filter_generator(self, filter=None):
        _status = None
        _saftiy_flag = ' and + status != unknown'

        if 'start' is self._action_name:
            _status = 'status = down'
        elif 'stop' is self._action_name:
            _status = 'status = up'
        elif 'activate' is self._action_name:
            _status = 'status != up'
        elif 'deactivate' is self._action_name:
            _status = 'status = up'
        elif 'delete' is self._action_name:
            if 'storagedomains' in self.entt_type:
                _status = 'status != active'
            else:
                _status = 'status != up'
        elif 'kill' is self._action_name:
            _status = 'status = waitforlaunch'
        elif 'attach' is self._action_name:
            _status = 'status != active'
        elif 'detach' is self._action_name:
            _status = 'status = active'
        else:
            _status = ""

        # adding saftiy falg in oreder to deselect uknown object
        try:
            _status + _saftiy_flag
        except Exception:
            raise Exception("action must be set")

        if filter:
            if _status:
                self.log.debug('filter = {0} and {1}'.format(_status, filter))
                return '{0} and {1}'.format(_status, filter)
            else:
                self.log.debug('filter = {0}'.format(filter))
                return '{0}'.format(filter)
        else:
            self.log.debug('filter = {0}'.format(_status))
            return _status

    def start(self, entt_obj=None):
        self.log.debug('about to start {0}'.format(entt_obj))
        try:
            entt_obj.start()
            return True
        except Exception as ex:
            self.log.error('{0} was failed to start'.format(entt_obj))
            return entt_obj
        finally:
            self.log.debug('{0} was started'.format(entt_obj))

    def stop(self, entt_obj=None):
        self.log.debug("about to stop {0}".format(entt_obj))
        try:
            entt_obj.stop()
            return True
        except Exception as ex:
            self.log.error('{0} was failed to stop due to {1}'.format(entt_obj, ex))
            return entt_obj
        finally:
            self.log.debug('{0} was stop'.format(entt_obj))

    def delete(self, entt_obj=None):
        self.log.debug('about to delete %s' % entt_obj)
        try:
            entt_obj.delete()
            return True
        except Exception as ex:
            return entt_obj
        finally:
            self.log.debug('%s was delete' % entt_obj)

    def detach(self, entt_obj=None):
        self.log.debug('about to detach %s' % entt_obj)
        try:
            entt_obj.detach()
            return True
        except Exception as ex:
            return entt_obj
        finally:
            self.log.debug('%s was detach' % entt_obj)

    def update(self, entt_obj=None):
        self.log.debug('about to update %s' % entt_obj)
        try:
            entt_obj.update()
            return True
        except Exception as ex:
            return entt_obj
        finally:
            self.log.debug('%s was update' % entt_obj)

    def deactivate(self, entt_obj=None):
        if entt_obj is None:
            return False
        try:
            self.log.debug('About to stop %s' % entt_obj)
            entt_obj.deactivate()
            return True
        except Exception as ex:
            return entt_obj
        finally:
            self.log.debug('%s was done' % entt_obj)

    def activate(self, entt_obj=None):
        print 'activate host'
        try:
            entt_obj.activate()
            return True
        except Exception as ex:
            return entt_obj
        finally:
            self.log.debug('{0} were done'.format(entt_obj))

    def add(self, entt_obj=None):
        if entt_obj is None:
            return False
        try:
            # self.apiObj.datacenters.get(name=self.datacenter).storagedomains.add(entt_obj)
            self.log.debug('About to stop %s' % entt_obj)
            entt_obj.add(entt_obj)
            return True
        except Exception as ex:
            return entt_obj
        finally:
            self.log.debug('%s was done' % entt_obj)



    def action_factory(self, actiongen):
        """
        this factory builds references to what action is going to be executed.
        @param actiongen:
        @return: method
        """
        try:
            if actiongen is 'start':
                return self.start
            elif actiongen is 'kill':
                return self.stop
            elif actiongen is 'stop':
                return self.stop
            elif actiongen is 'deactivate':
                return self.deactivate
            elif actiongen is 'activate':
                return self.activate
            elif actiongen is 'delete':
                return self.delete
            elif actiongen is 'detach':
                return self.detach
            elif actiongen is 'update':
                return self.update
            # TODO: handle this one
            elif actiongen is None:
                return None
        except Exception as e:
            self.log.error("action: {0} were failed: {1}".format(actiongen, e))
            
    def get_action(self):
        return self._action_name

    def set_action(self, action):
        self._action = self.action_factory(action)