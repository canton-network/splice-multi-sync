// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import dayjs from 'dayjs';
import { useState } from 'react';
import { useStore } from '@tanstack/react-form';
import { dateTimeFormatISO } from '@canton-network/splice-common-frontend-utils';
import { ContractId } from '@daml/types';
import { RegisteredSynchronizer } from '@daml.js/splice-amulet/lib/Splice/DecentralizedSynchronizer';
import { ActionRequiringConfirmation } from '@daml.js/splice-dso-governance/lib/Splice/DsoRules';

import { useAppForm } from '../../hooks/form';
import { useDsoInfos } from '../../contexts/SvContext';
import { useProposalMutation } from '../../hooks/useProposalMutation';
import {
  RegistrationLookupBy,
  useSynchronizerRegistrationLookup,
} from '../../hooks/useSynchronizerRegistrationLookup';
import { createProposalActions, getInitialExpiration } from '../../utils/governance';
import { CommonProposalFormData } from '../../utils/types';
import {
  CREATE_PROPOSAL_LABEL_EFFECTIVE_AT,
  CREATE_PROPOSAL_LABEL_PROPOSAL_SUMMARY,
  CREATE_PROPOSAL_LABEL_PROPOSAL_TYPE,
  CREATE_PROPOSAL_LABEL_SUPPORTING_URL,
  CREATE_PROPOSAL_LABEL_THRESHOLD_DEADLINE,
  SUPPORTING_URL_PLACEHOLDER,
  THRESHOLD_DEADLINE_SUBTITLE,
} from '../../utils/constants';
import {
  validateContractId,
  validateEffectiveDate,
  validateExpiration,
  validateExpiryEffectiveDate,
  validateSummary,
  validateSynchronizerId,
  validateUrl,
} from './formValidators';
import { EffectiveDateField } from '../form-components/EffectiveDateField';
import { ProposalSubmissionError } from '../form-components/ProposalSubmissionError';
import { ProposalSummary } from '../governance/ProposalSummary';
import { FormLayout } from './FormLayout';
import { ResolvedSynchronizerRegistrationFields } from './ResolvedSynchronizerRegistration';

interface ExtraFormField {
  lookupBy: RegistrationLookupBy;
  /** A synchronizer id or a registration contract id, depending on lookupBy. */
  registration: string;
}

export type ArchiveSynchronizerRegistrationFormData = CommonProposalFormData & ExtraFormField;

const lookupByOptions: { key: string; value: RegistrationLookupBy }[] = [
  { key: 'Synchronizer ID', value: 'synchronizer-id' },
  { key: 'Registration Contract ID', value: 'contract-id' },
];

const validateRegistrationInput = (by: RegistrationLookupBy, value: string) =>
  by === 'synchronizer-id'
    ? validateSynchronizerId(value.trim())
    : validateContractId(value.trim());

export const ArchiveSynchronizerRegistrationForm: React.FC = _ => {
  const dsoInfosQuery = useDsoInfos();
  const initialExpiration = getInitialExpiration(dsoInfosQuery.data);
  const initialEffectiveDate = dayjs(initialExpiration).add(1, 'day');
  const [showConfirmation, setShowConfirmation] = useState(false);
  const mutation = useProposalMutation();
  const lookup = useSynchronizerRegistrationLookup();
  const idPrefix = 'archive-synchronizer-registration';

  const createProposalAction = createProposalActions.find(
    a => a.value === 'SRARC_ArchiveSynchronizerRegistration'
  );

  const defaultValues: ArchiveSynchronizerRegistrationFormData = {
    action: createProposalAction?.name || '',
    expiryDate: initialExpiration.format(dateTimeFormatISO),
    effectiveDate: {
      type: 'custom',
      effectiveDate: initialEffectiveDate.format(dateTimeFormatISO),
    },
    url: '',
    summary: '',
    lookupBy: 'synchronizer-id',
    registration: '',
  };

  const form = useAppForm({
    defaultValues,

    onSubmit: async ({ value }) => {
      // The field's async validator resolved it; without it there is nothing to target.
      const resolved = lookup.getResolved();
      if (!resolved) return;
      const action: ActionRequiringConfirmation = {
        tag: 'ARC_DsoRules',
        value: {
          dsoAction: {
            tag: 'SRARC_ArchiveSynchronizerRegistration',
            value: {
              registeredSynchronizerCid: resolved.contractId as ContractId<RegisteredSynchronizer>,
            },
          },
        },
      };

      if (!showConfirmation) {
        setShowConfirmation(true);
      } else {
        await mutation.mutateAsync({ formData: value, action }).catch(e => {
          console.error(`Failed to submit proposal`, e);
        });
      }
    },

    validators: {
      onChange: ({ value }) => {
        return validateExpiryEffectiveDate({
          expiration: value.expiryDate,
          effectiveDate: value.effectiveDate.effectiveDate,
        });
      },
    },
  });

  const lookupBy = useStore(form.store, state => state.values.lookupBy);

  return (
    <FormLayout
      form={form}
      id={`${idPrefix}-form`}
      actionName={form.state.values.action}
      isReviewStep={showConfirmation}
    >
      {showConfirmation && lookup.resolved ? (
        <ProposalSummary
          actionName={form.state.values.action}
          url={form.state.values.url}
          summary={form.state.values.summary}
          expiryDate={form.state.values.expiryDate}
          effectiveDate={form.state.values.effectiveDate.effectiveDate}
          formType="archive-synchronizer-registration"
          registration={lookup.resolved}
          onEdit={() => setShowConfirmation(false)}
          onSubmit={() => {}}
        />
      ) : (
        <>
          <form.AppField name="action">
            {field => (
              <field.ProposalTypeField
                id={`${idPrefix}-action`}
                title={CREATE_PROPOSAL_LABEL_PROPOSAL_TYPE}
              />
            )}
          </form.AppField>

          <form.AppField
            name="lookupBy"
            listeners={{
              onChange: () => {
                // The input means something else now, so it is looked up afresh.
                form.resetField('registration');
                lookup.reset();
              },
            }}
          >
            {field => (
              <field.SelectField
                title="Find the Registration By"
                id={`${idPrefix}-lookup-by`}
                options={lookupByOptions}
              />
            )}
          </form.AppField>

          <form.AppField
            name="registration"
            validators={{
              onBlur: ({ value, fieldApi }) =>
                validateRegistrationInput(fieldApi.form.getFieldValue('lookupBy'), value),
              onChange: ({ value, fieldApi }) =>
                validateRegistrationInput(fieldApi.form.getFieldValue('lookupBy'), value),
              onChangeAsyncDebounceMs: 500,
              onChangeAsync: ({ value, fieldApi }) =>
                lookup.resolveAndValidate(fieldApi.form.getFieldValue('lookupBy'), value),
              onBlurAsync: ({ value, fieldApi }) =>
                lookup.resolveAndValidate(fieldApi.form.getFieldValue('lookupBy'), value),
            }}
          >
            {field =>
              lookupBy === 'synchronizer-id' ? (
                <field.TextField
                  title="Synchronizer ID"
                  id={`${idPrefix}-synchronizer-id`}
                  subtitle="The id of the dedicated synchronizer to offboard, as name::fingerprint"
                  onChange={() => lookup.reset()}
                />
              ) : (
                <field.TextField
                  title="Registration Contract ID"
                  id={`${idPrefix}-contract-id`}
                  subtitle="The contract id of the registration to archive. Use it to offboard a duplicate registration, which the synchronizer id does not reach."
                  onChange={() => lookup.reset()}
                />
              )
            }
          </form.AppField>

          {lookup.resolved && (
            <ResolvedSynchronizerRegistrationFields
              id={`${idPrefix}-resolved`}
              registration={lookup.resolved}
            />
          )}

          <form.AppField
            name="expiryDate"
            validators={{
              onChange: ({ value }) => validateExpiration(value),
              onBlur: ({ value }) => validateExpiration(value),
            }}
          >
            {field => (
              <field.DateField
                title={CREATE_PROPOSAL_LABEL_THRESHOLD_DEADLINE}
                description={THRESHOLD_DEADLINE_SUBTITLE}
                id={`${idPrefix}-expiry-date`}
              />
            )}
          </form.AppField>

          <form.AppField
            name="effectiveDate"
            validators={{
              onChange: ({ value }) => validateEffectiveDate(value),
              onBlur: ({ value }) => validateEffectiveDate(value),
            }}
            children={_ => (
              <EffectiveDateField
                title={CREATE_PROPOSAL_LABEL_EFFECTIVE_AT}
                initialEffectiveDate={initialEffectiveDate.format(dateTimeFormatISO)}
                id={`${idPrefix}-effective-date`}
              />
            )}
          />

          <form.AppField
            name="summary"
            validators={{
              onBlur: ({ value }) => validateSummary(value),
              onChange: ({ value }) => validateSummary(value),
            }}
          >
            {field => (
              <field.ProposalSummaryField
                id={`${idPrefix}-summary`}
                title={CREATE_PROPOSAL_LABEL_PROPOSAL_SUMMARY}
              />
            )}
          </form.AppField>

          <form.AppField
            name="url"
            validators={{
              onBlur: ({ value }) => validateUrl(value),
              onChange: ({ value }) => validateUrl(value),
            }}
          >
            {field => (
              <field.TextField
                title={CREATE_PROPOSAL_LABEL_SUPPORTING_URL}
                id={`${idPrefix}-url`}
                muiTextFieldProps={{ placeholder: SUPPORTING_URL_PLACEHOLDER }}
              />
            )}
          </form.AppField>
        </>
      )}

      <form.AppForm>
        <ProposalSubmissionError error={mutation.error} />

        <form.FormErrors />

        <form.FormControls
          showConfirmation={showConfirmation}
          onEdit={() => setShowConfirmation(false)}
        />
      </form.AppForm>
    </FormLayout>
  );
};
