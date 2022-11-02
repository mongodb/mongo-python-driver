#!/bin/bash

if [ -z "${AZUREKMS_VMNAME_PREFIX:-}" ]; then
    echo "missing AZUREKMS_VMNAME_PREFIX"
  fi
  if [ -z "${AZUREKMS_CLIENTID:-}" ]; then
    echo "missing AZUREKMS_CLIENTID"
  fi
  if [ -z "${AZUREKMS_TENANTID:-}" ]; then
    echo "missing AZUREKMS_TENANTID"
  fi
  if [ -z "${AZUREKMS_SECRET:-}" ]; then
    echo "missing AZUREKMS_SECRET"
  fi
  if [ -z "${AZUREKMS_DRIVERS_TOOLS:-}" ]; then
    echo "missing AZUREKMS_DRIVERS_TOOLS"
  fi
  if [ -z "${AZUREKMS_RESOURCEGROUP:-}" ]; then
    echo "missing AZUREKMS_RESOURCEGROUP"
  fi
  if [ -z "${AZUREKMS_PUBLICKEYPATH:-}" ]; then
    echo "missing AZUREKMS_PUBLICKEYPATH"
  fi
  if [ -z "${AZUREKMS_PRIVATEKEYPATH:-}" ]; then
    echo "missing AZUREKMS_PRIVATEKEYPATH"
  fi
  if [ -z "${AZUREKMS_SCOPE:-}" ]; then
    echo "missing AZUREKMS_SCOPE"
  fi
