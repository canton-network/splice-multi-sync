// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { useQuery, UseQueryResult } from '@tanstack/react-query';
import { LookupSynchronizerRegistrationResponse } from '@canton-network/sv-openapi';

import { useSvAdminClient } from '../contexts/SvAdminServiceContext';

/** The registration of `synchronizerId`, or undefined when it is not registered.
 *
 * Used to warn a proposer that a synchronizer id is already taken. Disabled until the id
 * looks well formed, so every keystroke does not hit the endpoint.
 */
export const useSynchronizerRegistration = (
  synchronizerId: string,
  enabled: boolean
): UseQueryResult<LookupSynchronizerRegistrationResponse | undefined> => {
  const { lookupSynchronizerRegistration } = useSvAdminClient();
  return useQuery({
    queryKey: ['lookupSynchronizerRegistration', synchronizerId],
    queryFn: async () => await lookupSynchronizerRegistration(synchronizerId),
    enabled: enabled && synchronizerId.length > 0,
  });
};
