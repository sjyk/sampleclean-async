#!/usr/bin/python

# Generate a key, self-signed certificate, and certificate request.
# Usage: gencert hostname [hostname...]
#
# When more than one hostname is provided, a SAN (Subject Alternate Name)
# certificate and request are generated.  The first hostname is used as the
# primary CN for the request.
#
# Author: James E. Blair <jeblair@berkeley.edu>  2010-06-18
# With help from this thread:
# http://www.mail-archive.com/openssl-users@openssl.org/msg47641.html

import os,sys
import subprocess
import tempfile

OPENSSL_CNF="""
[ req ]
default_bits            = 2048
default_md              = sha256
distinguished_name      = req_distinguished_name
prompt = no
%(req)s

[ req_distinguished_name ]
C=US
ST=California
L=Berkeley
O=University of California at Berkeley
OU=EECS
%(cn)s

[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[ v3_ca ]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always
basicConstraints = CA:true
subjectAltName = @alt_names

[ alt_names ]
%(alt)s
"""

SAN_REQ = """
x509_extensions = v3_ca # The extentions to add to the self signed cert
req_extensions = v3_req # The extensions to add to a certificate request
"""

def run(args):
    p = subprocess.Popen(args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         close_fds=True)
    p.stdin.close()
    while True:
        o = p.stdout.read(1)
        if not o: break
        sys.stdout.write(o)
        sys.stdout.flush()
    r = p.wait()
    if r:
        raise Exception('Error running %s'%args)

if __name__=="__main__":
    names = sys.argv[1:]
    if not names:
        print "Usage: gencert hostname [hostname...]"
        print
        print "  Please provide at least one hostname on the command line."
        print "  Mulitple hostnames may be provided to generate a SAN request."
        print
        sys.exit(1)
    params = dict(req='', dn='', alt='')
    if len(names)>1:
        # SAN
        san_names = ""
        for i,x in enumerate(names):
            san_names += "DNS.%s = %s\n" % (i,x)
        params['req']=SAN_REQ
        params['alt']=san_names
        sanfn = '-san'
    else:
        sanfn = ''
    params['cn']='CN=%s'%names[0]
    keyfile = '/etc/pki/tls/private/%s%s.key' % (names[0], sanfn)
    crtfile = '/etc/pki/tls/certs/%s%s.cert' % (names[0], sanfn)
    csrfile = '/etc/pki/tls/certs/%s%s.csr' % (names[0], sanfn)
    (fh, cnffile) = tempfile.mkstemp()

    os.write(fh, OPENSSL_CNF%params)
    os.close(fh)

    if os.path.exists(crtfile):
        print "Certificate file exists, aborting"
        print "  ", crtfile
        sys.exit(1)

    if os.path.exists(csrfile):
        print "Certificate request file exists, aborting"
        print "  ", csrfile
        sys.exit(1)

    if os.path.exists(keyfile):
        print "Key file exists, skipping key generation"
    else:
        run(['openssl', 'genrsa', '-out', keyfile, '2048'])
        os.chmod(keyfile, 0400)
    run(['openssl', 'req', '-config', cnffile, '-new', '-nodes', '-key', keyfile, '-out', csrfile])
    run(['openssl', 'req', '-config', cnffile, '-new', '-nodes', '-key', keyfile, '-out', crtfile, '-x509'])
    run(['openssl', 'req', '-in', csrfile, '-text'])

    os.unlink(cnffile)
