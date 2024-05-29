"""

Copyright (c) 2020 Alex Forencich

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""

import logging
from typing import List, Tuple

import cocotb
from cocotb.queue import Queue
from cocotb.triggers import Event
from cocotb.xt_printer import xt_print

from .bridge import Bridge, RootPort, SwitchUpstreamPort, SwitchDownstreamPort
from .tlp import Tlp, TlpType
from .utils import PcieId


class SwitchPort:
    def __init__(self, bridge: Bridge):
        assert isinstance(bridge, Bridge)
        self.name = f"{bridge.name}SwitchPort"
        self.bridge = bridge
        self.is_upstream = False

        self.ingress_queue = Queue(1)

        self.tx_queues: List[Tuple[SwitchPort, Queue]] = []
        self.rx_queues: List[Queue] = []
        self.rx_event = Event()

        self.tx_handler = None
        self.tx_handler_name = None

    @classmethod
    def upstream(cls, bridge: Bridge):
        port = cls(bridge)
        port.is_upstream = True
        bridge.downstream_tx_name = f"SwitchPort to {port.bridge.name} downstream"
        bridge.downstream_tx_handler = port.ingress_queue.put
        port.tx_handler = bridge.downstream_recv
        port.tx_handler_name = f"bridge.{bridge.name}.downstream_recv"
        return port

    @classmethod
    def downstream(cls, bridge):
        assert isinstance(bridge, RootPort)
        port = cls(bridge)
        port.is_upstream = False
        bridge.upstream_tx_handler = port.ingress_queue.put
        port.tx_handler = bridge.upstream_recv
        port.tx_handler_name = f"bridge.{bridge.name}.upstream_recv"
        return port


class Switch:
    """Switch object, container for switch bridges and associated interconnect"""
    def __init__(self, name, *args, **kwargs):
        self.__dict__.setdefault('default_upstream_bridge', SwitchUpstreamPort)
        self.__dict__.setdefault('default_downstream_bridge', SwitchDownstreamPort)

        super().__init__(*args, **kwargs)
        xt_print(f"Create Switch {name}")
        self.name = name

        self.log = logging.getLogger(f"cocotb.pcie.{type(self).__name__}.{id(self)}")
        self.log.name = f"cocotb.pcie.{type(self).__name__}"

        self.switch_ports: List[SwitchPort] = []

        from cocotbext.pcie.core.bridge import HostBridge
        assert self.default_upstream_bridge == HostBridge
        self.upstream_bridge: HostBridge = self.default_upstream_bridge("TheHostBridge")
        self.upstream_port: SwitchPort = SwitchPort.upstream(self.upstream_bridge)
        xt_print(f"Setting up Switch {self.name}, it upstream bridge is TheHostBridge")
        self.add_switch_port(self.upstream_port)

        self.min_dev = 1
        self.endpoints = []

    @property
    def pcie_id(self):
        return self.upstream_bridge._pcie_id

    @property
    def bus_num(self):
        return self.pcie_id.bus

    @property
    def device_num(self):
        return self.pcie_id.device

    @property
    def function_num(self):
        return self.pcie_id.function

    def next_free_device_number(self):
        self.endpoints.sort(key=lambda x: (x.device_num, x.function_num))
        d = self.min_dev
        if not self.endpoints:
            return d
        for ep in self.endpoints:
            if ep.device_num > d:
                return d
            d = ep.device_num + 1
        if d < 32:
            return d
        return None

    def append_endpoint(self, ep: RootPort):
        assert isinstance(ep, RootPort)
        self.add_switch_port(SwitchPort.downstream(ep))
        self.endpoints.append(ep)
        self.endpoints.sort(key=lambda x: (x.device_num, x.function_num))
        return ep

    def add_endpoint(self, ep):
        ep.pcie_id = PcieId(0, self.next_free_device_number(), 0)
        return self.append_endpoint(ep)

    def make_port(self, bridge=None, port=None):
        if bridge is None:
            assert self.default_downstream_bridge == RootPort
            xt_print(f"Create RootPort for {self.name}")
            bridge = self.default_downstream_bridge("RootPort")

            # transfer configuration from upstream bridge
            bridge.pcie_cap.max_payload_size_supported = self.upstream_bridge.pcie_cap.max_payload_size_supported
            bridge.pcie_cap.extended_tag_supported = self.upstream_bridge.pcie_cap.extended_tag_supported

        if port is not None:
            bridge.set_downstream_port(port)
        bridge.pri_bus_num = 0
        bridge.sec_bus_num = 0
        bridge.sub_bus_num = 0
        return self.add_endpoint(bridge)

    def set_upstream_bridge(self, bridge: Bridge):
        assert isinstance(bridge, Bridge)
        self.upstream_bridge = bridge
        bridge.downstream_tx_name = f"{self.upstream_port.name}.ingress_queue"
        bridge.downstream_tx_handler = self.upstream_port.ingress_queue.put
        self.upstream_port.tx_handler = bridge.downstream_recv

    def set_upstream_port(self, port):
        self.upstream_bridge.set_upstream_port(port)

    def add_switch_port(self, port: SwitchPort):
        assert isinstance(port, SwitchPort)
        self.switch_ports.append(port)
        cocotb.start_soon(self._run_routing(port))
        cocotb.start_soon(self._run_arbitration(port))

        for k in range(len(self.switch_ports)-1):
            tx_queue = Queue()
            rx_queue = Queue()
            port.tx_queues.append((self.switch_ports[k], tx_queue))
            port.rx_queues.append(rx_queue)
            self.switch_ports[k].rx_queues.append(tx_queue)
            self.switch_ports[k].tx_queues.append((port, rx_queue))

    def connect(self, port):
        self.upstream_bridge.upstream_port.connect(port)

    async def _run_routing(self, port: SwitchPort):
        assert isinstance(port, SwitchPort)
        while True:
            tlp = await port.ingress_queue.get()
            assert isinstance(tlp, Tlp), type(tlp)
            xt_print(f"!!!!!! got here !!!!!!!! 4 Switch {self.name} src port {port.name} routing, ingress_queue get tlp, "
                     f"total {len(port.tx_queues)} tx_queues:\n{tlp.to_str()}")

            tlp.ingress_port = port.bridge

            ok = False

            for other, tx_queue in port.tx_queues:
                xt_print(f"!!!!!! got here !!!!!!!! 5 Switch {self.name} tx_queue dst port {other.name}, bridge {other.bridge.name}, "
                         f"other is upstream {other.is_upstream}, other bridge bus ({other.bridge.sec_bus_num},{other.bridge.sub_bus_num})")
                if other.bridge.match_tlp(tlp):
                    xt_print(f"!!!!!! got here !!!!!!!! 6 Switch {self.name}  dst port {other.name}, other match_tlp")
                    # TLP directed to bridge
                    await tx_queue.put(tlp)
                    other.rx_event.set()
                    ok = True
                    break
                elif other.is_upstream:
                    xt_print(f"!!!!!! got here !!!!!!!! 6 Switch {self.name}  dst port {other.name}, match is_upstream")
                    if not other.bridge.match_tlp_secondary(tlp):
                        xt_print(f"!!!!!! got here !!!!!!!! 6+ Switch {self.name}  dst port {other.name}, not match match_tlp_secondary")
                        # TLP routed through upstream bridge
                        await tx_queue.put(tlp)
                        other.rx_event.set()
                        ok = True
                        break
                else:
                    xt_print(f"!!!!!! got here !!!!!!!! 6 Switch {self.name}  dst port {other.name}, match else")
                    if other.bridge.match_tlp_secondary(tlp):
                        xt_print(f"!!!!!! got here !!!!!!!! 6+ Switch {self.name}  dst port {other.name}, match match_tlp_secondary")
                        # TLP routed through downstream bridge
                        await tx_queue.put(tlp)
                        other.rx_event.set()
                        ok = True
                        break
            xt_print(f"!!!!!! got here !!!!!!!! 6++ Switch {self.name}  dst port {other.name}, ok={ok}")

            if ok:
                continue

            tlp.release_fc()

            if tlp.fmt_type in {TlpType.CFG_READ_0, TlpType.CFG_WRITE_0}:
                # Config type 0
                self.log.warning("Switch Failed to route config type 0 TLP: %r", tlp)
            elif tlp.fmt_type in {TlpType.CFG_READ_1, TlpType.CFG_WRITE_1}:
                # Config type 1
                self.log.warning("Switch Failed to route config type 1 TLP: %r", tlp)
            elif tlp.fmt_type in {TlpType.CPL, TlpType.CPL_DATA, TlpType.CPL_LOCKED, TlpType.CPL_LOCKED_DATA}:
                # Completion
                self.log.warning("Unexpected completion: failed to route completion: %r", tlp)
                continue  # no UR response for completion
            elif tlp.fmt_type in {TlpType.IO_READ, TlpType.IO_WRITE}:
                # IO read/write
                self.log.warning("No address match: IO request could not be routed: %r", tlp)
            elif tlp.fmt_type in {TlpType.MEM_READ, TlpType.MEM_READ_64}:
                # Memory read/write
                self.log.warning("No address match: memory read request could not be routed: %r", tlp)
            elif tlp.fmt_type in {TlpType.MEM_WRITE, TlpType.MEM_WRITE_64}:
                # Memory read/write
                self.log.warning("No address match: memory write request could not be routed: %r", tlp)
                continue  # no UR response for write request
            else:
                raise Exception("TODO")

            # Unsupported request
            cpl = Tlp.create_ur_completion_for_tlp(tlp, port.bridge.pcie_id)
            self.log.debug("UR Completion: %r", cpl)
            xt_print(f"!!!!!! got here !!!!!!!! 6+++ Switch {self.name}  port {port.name}, dst port {other.name}, handler {port.tx_handler_name},create UR Completion:\n{cpl.to_str()}")
            await port.tx_handler(cpl)

    async def _run_arbitration(self, port):
        while True:
            await port.rx_event.wait()
            port.rx_event.clear()

            while True:
                ok = False

                for rx_queue in port.rx_queues:
                    if rx_queue.empty():
                        continue
                    tlp = await rx_queue.get()
                    await port.tx_handler(tlp)
                    ok = True

                if not ok:
                    break
