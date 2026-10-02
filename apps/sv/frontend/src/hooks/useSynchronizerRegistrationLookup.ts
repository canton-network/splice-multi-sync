// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { useRef, useState } from 'react';
import { Contract } from '@canton-network/splice-common-frontend-utils';
import {
  GovernanceParameters,
  RegisteredSynchronizer,
} from '@daml.js/splice-amulet/lib/Splice/DecentralizedSynchronizer';
import { VoteRequest } from '@daml.js/splice-dso-governance/lib/Splice/DsoRules';
import { validateContractId, validateSynchronizerId } from '../components/forms/formValidators';
import { useSvAdminClient } from '../contexts/SvAdminServiceContext';
import { useListDsoRulesVoteRequests } from './useListVoteRequests';

export interface ResolvedSynchronizerRegistration {
  contractId: string;
  synchronizerId: string;
  operator: string;
  governanceParameters: GovernanceParameters;
}

/** A registration is found by its synchronizer id, or by contract id to reach a duplicate. */
export type RegistrationLookupBy = 'synchronizer-id' | 'contract-id';

/** The contract id an open offboard or set-parameters vote pins, if it is one of those. */
export const pinnedRegistrationCid = (voteRequest: VoteRequest): string | undefined => {
  const action = voteRequest.action;
  if (action.tag !== 'ARC_DsoRules') return undefined;
  const dsoAction = action.value.dsoAction;
  switch (dsoAction.tag) {
    case 'SRARC_ArchiveSynchronizerRegistration':
    case 'SRARC_SetSynchronizerGovernanceParameters':
      return dsoAction.value.registeredSynchronizerCid;
    default:
      return undefined;
  }
};

interface SynchronizerRegistrationLookup {
  /** For rendering. */
  resolved: ResolvedSynchronizerRegistration | undefined;
  /** For submitting: current even before the re-render that follows a lookup. */
  getResolved: () => ResolvedSynchronizerRegistration | undefined;
  /** An async field validator: resolves the registration, or returns why it cannot be targeted. */
  resolveAndValidate: (by: RegistrationLookupBy, value: string) => Promise<string | undefined>;
  reset: () => void;
}

// Offboard and set-parameters votes pin a registration by contract id, so proposing one starts
// with finding that contract. Unlike registering, not finding it is an error here: there is
// nothing to target. A failed lookup blocks too, since without it there is no contract id.
export const useSynchronizerRegistrationLookup = (): SynchronizerRegistrationLookup => {
  const { lookupSynchronizerRegistration, lookupSynchronizerRegistrationByContractId } =
    useSvAdminClient();
  const voteRequestsQuery = useListDsoRulesVoteRequests();
  const [resolved, setResolvedState] = useState<ResolvedSynchronizerRegistration | undefined>();
  const resolvedRef = useRef<ResolvedSynchronizerRegistration | undefined>(undefined);
  // The input the current registration was resolved from.
  const resolvedInput = useRef('');
  const setResolved = (
    registration: ResolvedSynchronizerRegistration | undefined,
    inputKey: string = ''
  ) => {
    resolvedRef.current = registration;
    resolvedInput.current = registration ? inputKey : '';
    setResolvedState(registration);
  };
  // The input whose lookup is current, so a slower, older lookup cannot overwrite it.
  const latestInput = useRef('');

  const resolveAndValidate = async (by: RegistrationLookupBy, value: string) => {
    const input = value.trim();
    const inputKey = `${by}:${input}`;
    latestInput.current = inputKey;
    // Submitting runs every validator again, so the same input is often looked up twice. It
    // keeps its registration meanwhile: the form must not see it disappear and come back.
    if (resolvedInput.current !== inputKey) setResolved(undefined);

    // The field's synchronous validator reports a malformed input.
    const syntaxError =
      by === 'synchronizer-id' ? validateSynchronizerId(input) : validateContractId(input);
    if (syntaxError) return undefined;

    let contract;
    try {
      contract =
        by === 'synchronizer-id'
          ? (await lookupSynchronizerRegistration(input))?.registration.contract
          : (await lookupSynchronizerRegistrationByContractId(input)).registration;
    } catch (e) {
      if (latestInput.current !== inputKey) return undefined;
      console.error('Failed to look up the synchronizer registration', e);
      setResolved(undefined);
      return 'Could not look up the registration. Try again.';
    }
    if (latestInput.current !== inputKey) return undefined;

    if (!contract) {
      setResolved(undefined);
      return by === 'synchronizer-id'
        ? 'No registration found for this synchronizer id.'
        : 'No active registration has this contract id.';
    }

    const registration = Contract.decodeOpenAPI(contract, RegisteredSynchronizer);

    // Executing either vote archives the registration, so a second open vote on the same
    // contract would fail when executed.
    const targeted = (voteRequestsQuery.data ?? []).some(
      vr => pinnedRegistrationCid(vr.payload) === registration.contractId
    );
    if (targeted) {
      setResolved(undefined);
      return 'Another open proposal already targets this registration. Only one of them can be executed.';
    }

    // The same contract found again is kept as it is, so nothing that depends on it re-runs.
    if (resolvedRef.current?.contractId === registration.contractId) {
      resolvedInput.current = inputKey;
      return undefined;
    }
    setResolved(
      {
        contractId: registration.contractId,
        synchronizerId: registration.payload.synchronizerId,
        operator: registration.payload.operator,
        governanceParameters: registration.payload.governanceParameters,
      },
      inputKey
    );
    return undefined;
  };

  const reset = () => {
    latestInput.current = '';
    setResolved(undefined);
  };

  return { resolved, getResolved: () => resolvedRef.current, resolveAndValidate, reset };
};
