// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, test, vi } from 'vitest';
import userEvent from '@testing-library/user-event';
import { ArchiveSynchronizerRegistrationForm } from '../../../components/forms/ArchiveSynchronizerRegistrationForm';
import { Wrapper } from '../../helpers';
import {
  activeSynchronizerRegistration,
  activeSynchronizerRegistrationPayload,
} from '../../mocks/constants';

// The lookups need an authenticated SV admin client, which the render wrapper does not set
// up, so the sources the form reads are mocked and the component's own logic tested.
const mockLookupById = vi.fn();
const mockLookupByCid = vi.fn();
const mockVoteRequests = vi.fn();
vi.mock('../../../contexts/SvAdminServiceContext', async importOriginal => ({
  ...(await importOriginal<typeof import('../../../contexts/SvAdminServiceContext')>()),
  useSvAdminClient: () => ({
    lookupSynchronizerRegistration: mockLookupById,
    lookupSynchronizerRegistrationByContractId: mockLookupByCid,
  }),
}));
vi.mock('../../../hooks/useListVoteRequests', () => ({
  useListDsoRulesVoteRequests: () => mockVoteRequests(),
}));

const synchronizerId = activeSynchronizerRegistrationPayload.synchronizerId;
const operator = activeSynchronizerRegistrationPayload.operator;
const registrationCid = activeSynchronizerRegistration.contract_id;

const openVoteOn = (tag: string, registeredSynchronizerCid: string) => ({
  data: [
    {
      payload: {
        action: {
          tag: 'ARC_DsoRules',
          value: { dsoAction: { tag, value: { registeredSynchronizerCid } } },
        },
      },
    },
  ],
});

beforeEach(() => {
  vi.clearAllMocks();
  mockLookupById.mockResolvedValue({ registration: { contract: activeSynchronizerRegistration } });
  mockLookupByCid.mockResolvedValue({ registration: activeSynchronizerRegistration });
  mockVoteRequests.mockReturnValue({ data: [] });
});

const renderForm = () =>
  render(
    <Wrapper>
      <ArchiveSynchronizerRegistrationForm />
    </Wrapper>
  );

// Blurring runs the lookup without waiting out the typing debounce.
const enter = async (user: ReturnType<typeof userEvent.setup>, testId: string, value: string) => {
  await user.type(screen.getByTestId(testId), value);
  await user.click(screen.getByTestId('archive-synchronizer-registration-action'));
};

describe('Offboard Dedicated Synchronizer Form', () => {
  test('renders the form, looking the registration up by synchronizer id', () => {
    renderForm();

    expect(screen.getByTestId('archive-synchronizer-registration-form')).toBeInTheDocument();
    expect(screen.getByTestId('archive-synchronizer-registration-action').textContent).toBe(
      'Offboard Dedicated Synchronizer'
    );
    expect(
      screen.getByTestId('archive-synchronizer-registration-synchronizer-id')
    ).toBeInTheDocument();
    expect(
      screen.queryByTestId('archive-synchronizer-registration-contract-id')
    ).not.toBeInTheDocument();
  });

  test('shows the registration a synchronizer id resolves to', async () => {
    const user = userEvent.setup();
    renderForm();

    await enter(user, 'archive-synchronizer-registration-synchronizer-id', synchronizerId);

    await screen.findByTestId('archive-synchronizer-registration-resolved');
    expect(mockLookupById).toHaveBeenCalledWith(synchronizerId);
    expect(
      screen.getByTestId('archive-synchronizer-registration-resolved-contract-id-field').textContent
    ).toBe(registrationCid);
    expect(
      screen.getByTestId('archive-synchronizer-registration-resolved-operator-party-id')
    ).toHaveTextContent(operator);
  });

  test('rejects a synchronizer id that is not registered', async () => {
    mockLookupById.mockResolvedValue(undefined);
    const user = userEvent.setup();
    renderForm();

    await enter(user, 'archive-synchronizer-registration-synchronizer-id', synchronizerId);

    expect(await screen.findByText(/No registration found/)).toBeInTheDocument();
    expect(
      screen.queryByTestId('archive-synchronizer-registration-resolved')
    ).not.toBeInTheDocument();
  });

  test('does not look up a malformed synchronizer id', async () => {
    const user = userEvent.setup();
    renderForm();

    await enter(user, 'archive-synchronizer-registration-synchronizer-id', 'not-an-id');

    expect(await screen.findByText(/Invalid synchronizer id/)).toBeInTheDocument();
    expect(mockLookupById).not.toHaveBeenCalled();
  });

  test('a failing lookup blocks the proposal, since there is no contract to target', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    mockLookupById.mockRejectedValue(new Error('scan is unreachable'));
    const user = userEvent.setup();
    renderForm();

    await enter(user, 'archive-synchronizer-registration-synchronizer-id', synchronizerId);

    expect(await screen.findByText(/Could not look up the registration/)).toBeInTheDocument();
  });

  test('rejects a registration another open proposal already targets', async () => {
    mockVoteRequests.mockReturnValue(
      openVoteOn('SRARC_SetSynchronizerGovernanceParameters', registrationCid)
    );
    const user = userEvent.setup();
    renderForm();

    await enter(user, 'archive-synchronizer-registration-synchronizer-id', synchronizerId);

    expect(await screen.findByText(/Another open proposal/)).toBeInTheDocument();
  });

  test('looks a registration up by contract id, which reaches a duplicate', async () => {
    const user = userEvent.setup();
    renderForm();

    fireEvent.change(screen.getByTestId('archive-synchronizer-registration-lookup-by-dropdown'), {
      target: { value: 'contract-id' },
    });
    await enter(user, 'archive-synchronizer-registration-contract-id', registrationCid);

    await screen.findByTestId('archive-synchronizer-registration-resolved');
    expect(mockLookupByCid).toHaveBeenCalledWith(registrationCid);
    expect(mockLookupById).not.toHaveBeenCalled();
    expect(
      screen.getByTestId('archive-synchronizer-registration-resolved-synchronizer-id-field')
        .textContent
    ).toBe(synchronizerId);
  });

  test('rejects a contract id that is not an active registration', async () => {
    mockLookupByCid.mockResolvedValue({});
    const user = userEvent.setup();
    renderForm();

    fireEvent.change(screen.getByTestId('archive-synchronizer-registration-lookup-by-dropdown'), {
      target: { value: 'contract-id' },
    });
    await enter(user, 'archive-synchronizer-registration-contract-id', registrationCid);

    expect(
      await screen.findByText(/No active registration has this contract id/)
    ).toBeInTheDocument();
  });

  test('reviews the resolved registration', async () => {
    const user = userEvent.setup();
    renderForm();

    await user.type(
      screen.getByTestId('archive-synchronizer-registration-summary'),
      'Offboard a synchronizer'
    );
    await user.type(
      screen.getByTestId('archive-synchronizer-registration-url'),
      'https://example.com'
    );
    await enter(user, 'archive-synchronizer-registration-synchronizer-id', synchronizerId);
    await screen.findByTestId('archive-synchronizer-registration-resolved');

    const submitButton = screen.getByTestId('submit-button');
    await waitFor(() => expect(submitButton.getAttribute('disabled')).toBeNull());
    await user.click(submitButton);

    await screen.findByTestId('proposal-review');
    expect(screen.getByTestId('registeredSynchronizerCid-field').textContent).toBe(registrationCid);
    expect(screen.getByTestId('synchronizerId-field').textContent).toBe(synchronizerId);
  });
});
