from rhevm_utils import EngineSetup

try:
    """ Constrat API Connection """
    ovirt = EngineSetup('ovirt.engine.com',
                        'admin', 'internal', 'qum5net', True, True, 'qum5net',
                        version='4.1', logname='engine_controller', level='INFO')

    """ start all vms """
    ovirt.action_executor(data_center='*',
                          action='start',
                          entity_type='vms',
                          querystring='name = *scale*',
                          capacity=1000,
                          bulks=10,
                          delay=50,
                          ampup=9,
                          iterations=1000)

    """ start all vms """
    ovirt.action_executor(data_center='*',
                          action='deactivate',
                          entity_type='hosts',
                          querystring='name = *scale*',
                          capacity=1000,
                          bulks=10,
                          delay=50,
                          rampup=9,
                          iterations=1000)

except RuntimeError as e:
    print e
finally:
    if ovirt:
        ovirt.disconnect()
exit(0)
