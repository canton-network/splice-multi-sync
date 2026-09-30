// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Box } from '@mui/material';
import { MemberIdentifier } from '../beta';
import { IDENTIFIER_COMPACT_MAX_WIDTH_PX } from '../beta/identifierStyles';
import { ProposalReviewField } from '../governance/ProposalReviewField';
import type { ResolvedSynchronizerRegistration } from '../../hooks/useSynchronizerRegistrationLookup';
import { governanceParameterFields, governanceParameterKeys } from './governanceParameterFields';

interface ResolvedSynchronizerRegistrationProps {
  id: string;
  registration: ResolvedSynchronizerRegistration;
  /** Also list the registration's current governance parameters. */
  showParameters?: boolean;
}

/** What a proposal would target, shown before the SV proposes it. */
export const ResolvedSynchronizerRegistrationFields: React.FC<
  ResolvedSynchronizerRegistrationProps
> = ({ id, registration, showParameters = false }) => (
  <Box data-testid={id} sx={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
    <ProposalReviewField
      id={`${id}-synchronizer-id`}
      label="Synchronizer ID"
      value={registration.synchronizerId}
    />
    <ProposalReviewField
      id={`${id}-operator`}
      label="Synchronizer Operator"
      value={
        <MemberIdentifier
          partyId={registration.operator}
          isYou={false}
          size="small"
          maxWidth={IDENTIFIER_COMPACT_MAX_WIDTH_PX}
          data-testid={`${id}-operator-party-id`}
        />
      }
    />
    <ProposalReviewField
      id={`${id}-contract-id`}
      label="Registration Contract ID"
      value={registration.contractId}
    />
    {showParameters &&
      governanceParameterKeys.map(key => (
        <ProposalReviewField
          key={key}
          id={`${id}-current-${key}`}
          label={`Current ${governanceParameterFields[key].label}`}
          value={governanceParameterFields[key].toForm(registration.governanceParameters[key])}
        />
      ))}
  </Box>
);
