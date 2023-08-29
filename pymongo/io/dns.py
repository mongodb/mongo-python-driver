try:
    from typing import Optional, Union

    from dns import asyncresolver, rdataclass, rdatatype, resolver
    from dns._asyncbackend import Backend
    from dns.name import Name
    from dns.rdataclass import RdataClass
    from dns.rdatatype import RdataType
    from dns.resolver import NXDOMAIN, Answer, NoAnswer
    from greenletio import await_

    def resolve(
        qname: Union[Name, str],
        rdtype: Union[RdataType, str] = rdatatype.A,
        rdclass: Union[RdataClass, str] = rdataclass.IN,
        tcp: bool = False,
        source: Optional[str] = None,
        raise_on_no_answer: bool = True,
        source_port: int = 0,
        lifetime: Optional[float] = None,
        search: Optional[bool] = None,
    ) -> Answer:
        return await_(
            asyncresolver.resolve(
                qname,
                rdtype,
                rdclass,
                tcp,
                source,
                raise_on_no_answer,
                source_port,
                lifetime,
                search,
            )
        )

    def query(
        qname: Union[Name, str],
        rdtype: Union[RdataType, str] = rdatatype.A,
        rdclass: Union[RdataClass, str] = rdataclass.IN,
        tcp: bool = False,
        source: Optional[str] = None,
        raise_on_no_answer: bool = True,
        source_port: int = 0,
        lifetime: Optional[float] = None,
    ) -> Answer:
        return resolve(
            qname, rdtype, rdclass, tcp, source, raise_on_no_answer, source_port, lifetime, True
        )

except ImportError:
    from dns.resolver import NXDOMAIN, Answer, NoAnswer, query, resolve
